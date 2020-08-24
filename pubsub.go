package greddis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"
	"unsafe"
)

// TODO do multi-core benchmarks to ensure it scales ok

const (
	subTopic = iota + 1
	subPattern
)

// I can't really have more than one message per connection "active" at a time
// because the reader, the result etc are all tied to the connection (and we should really not
// be advancing the reader in another thread) - so the idea here with the pool and stuff
// is pretty stupid. I should add the message to the subscription manager and listen for it
// on the channel at the top of every loop of Listen()
type Message struct {
	Ctx     context.Context
	pattern []byte
	Result  *Result
	topic   []byte
}

func (m *Message) Init() {
	m.pattern = m.pattern[:0]
	m.topic = m.topic[:0]
}

func (m *Message) Pattern() string {
	return *(*string)(unsafe.Pointer(&m.pattern))
}

func (m *Message) Topic() string {
	return *(*string)(unsafe.Pointer(&m.topic))
}

func newMessage(res *Result, msgChan chan *Message) *Message {
	msg := &Message{
		Result:  res,
		pattern: make([]byte, 0, 100),
		topic:   make([]byte, 0, 100),
	}
	res.finish = func() {
		msgChan <- msg
	}
	return msg
}

type MessageChan <-chan *Message

type MessageChanMap map[string]MessageChan

type subscription struct {
	topic   string
	msgChan chan *Message
	subType int
}

func newSubscription(topic interface{}, bufLen int) *subscription {
	var t string
	var s int
	var msgChan chan *Message
	if bufLen > 0 {
		msgChan = make(chan *Message, bufLen)
	} else {
		msgChan = make(chan *Message)
	}
	switch d := topic.(type) {
	case string:
		t = d
		s = subTopic
	case RedisPattern:
		t = string(d)
		s = subPattern
	}
	return &subscription{
		topic:   t,
		msgChan: msgChan,
		subType: s,
	}
}

type subscriptionManager struct {
	listening bool
	chans     *sync.Map
	conn      *conn
	connPool  internalPool
	msgChan   chan *Message
	opts      *PubSubOpts
	writeLock *sync.Mutex
	readChan  chan string
}

func newSubscriptionManager(pool internalPool, opts *PubSubOpts) *subscriptionManager {
	// NewResult is called explicitly after a connection is established in the general case.
	// However subscriptionManager manages a connection set in subscriber mode, so when we instantiate
	// a subscriptionManager we do not have a connection yet and thus we pass in nil to NewResult
	// and swap it out later
	mngr := &subscriptionManager{
		opts:      opts,
		chans:     &sync.Map{},
		connPool:  pool,
		msgChan:   make(chan *Message),
		writeLock: &sync.Mutex{},
		readChan:  make(chan string),
		listening: false,
	}
	return mngr
}

func (s *subscriptionManager) getConn(ctx context.Context) (*conn, error) {
	var err error
	if s.conn == nil {
		s.conn, err = s.connPool.Get(ctx)
	}
	return s.conn, err
}

func (s *subscriptionManager) Listen(ctx context.Context, c *conn) {
	var count int
	var errs []error
	var err error
	array := NewArrayReader(c.r)
	for {
		select {
		case <-ctx.Done():
			log.Printf("%s\n", ctx.Err())
			return
		case m := <-s.msgChan:
			if m == nil {
				return
			}
			m.Init()
			c.conn.SetReadDeadline(time.Now().Add(s.opts.PingInterval))
			err = array.Init(ScanBulkString)
			// TODO when 1.15 is out, change this to this: https://github.com/golang/go/issues/31449
			if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
				c.conn.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout))
				if err := ping(ctx, c); err != nil {
					log.Printf("Ping error: %s", err)
					return
				}
				continue
			} else if err != nil {
				log.Printf("Array Init error: %s", err)
				return
			} else {
				c.conn.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout))
			}

			switch array.Next().SwitchOnNext() {
			case "pmessage":
				errs = append(errs, array.Next().Scan(&m.pattern))
				fallthrough
			case "message":
				errs = append(errs, array.Next().Scan(&m.topic))
			case "psubscribe":
				fallthrough
			case "subscribe":
				fallthrough
			case "punsubscribe":
				fallthrough
			case "unsubscribe":
				for i := 0; i < int(math.Ceil(float64(array.Len())/2)-1); i++ {
					errs = append(errs, array.Next().Expect(<-s.readChan))
					errs = append(errs, array.NextIs(ScanInteger).Scan(&count))
				}
			default:
				v, _ := array.r.r.Peek(array.r.r.Buffered())
				log.Printf(
					"We shouldn't be here :( value: '%s' buffered: %d\nContents: %s",
					array.r.String(),
					array.r.r.Buffered(),
					v,
				)
			}

			encounteredErr := false
			for _, err := range errs {
				if err != nil {
					encounteredErr = true
					log.Printf("Error: %s", err)
				}
			}
			errs = errs[:0]
			if encounteredErr {
				// TODO we should be trying to close the connection sanely here?
				return
			}
			if array.Len() > array.pos {
				array.Next()
				if array.Err() != nil {
					log.Printf("Next error: %s\n", array.Err())
				}
				s.dispatch(ctx, m)
			} else {
				go func() { s.msgChan <- m }()
			}
		}
	}
}

// Design publish command -> publish to channel that we're expecting a read, wait for return on second chan
func (s *subscriptionManager) Subscribe(ctx context.Context, topics ...interface{}) (MessageChanMap, error) {
	if len(topics) == 0 {
		return nil, errors.New("No topics given to subscribe to")
	}
	if !s.listening {
		conn, err := s.getConn(ctx)
		if err != nil {
			return nil, err
		}
		go s.Listen(ctx, conn)
		s.msgChan <- newMessage(NewResult(conn.r), s.msgChan)
		s.listening = true
	}
	subs := make([]*subscription, len(topics))
	chanMap := make(MessageChanMap, len(topics))

	s.writeLock.Lock()
	arrw := s.conn.arrw
	var subType int
	switch topics[0].(type) {
	case RedisPattern:
		subType = subPattern
		arrw.Init(len(topics) + 1).AddString("PSUBSCRIBE")
	case string:
		subType = subTopic
		arrw.Init(len(topics) + 1).AddString("SUBSCRIBE")
	}
	for i, topic := range topics {
		var sub *subscription
		switch d := topic.(type) {
		case RedisPattern:
			if subType != subPattern {
				s.writeLock.Unlock()
				return nil, ErrMixedTopicTypes
			}
		case string:
			if subType != subTopic {
				s.writeLock.Unlock()
				return nil, ErrMixedTopicTypes
			}
		default:
			s.writeLock.Unlock()
			return nil, fmt.Errorf("Wrong type! Expected RedisPattern or string, but received: %T", d)
		}
		sub = newSubscription(topic, s.opts.InitBufSize)
		arrw.AddString(sub.topic)
		subs[i] = sub
		chanMap[sub.topic] = sub.msgChan
	}
	var err error
	err = arrw.Flush()
	s.writeLock.Unlock()
	if err != nil {
		return nil, err
	}
	// Need to store the subscriptions after we flush the command so we only store valid subscriptions
	for _, sub := range subs {
		s.chans.Store(sub.topic, sub)
		s.readChan <- sub.topic
	}
	// TODO If subscriptionCount drops to 0, we can hand back the connection and kill our poor listener
	// this depends on the configuration of the subscriber portion
	return chanMap, nil
}

func (s *subscriptionManager) dispatch(ctx context.Context, m *Message) {
	m.Ctx = ctx
	key := m.Topic()
	if m.Pattern() != "" {
		key = m.Pattern()
	}
	if sub, ok := s.chans.Load(key); ok {
		select {
		case <-ctx.Done():
			return
		case sub.(*subscription).msgChan <- m:
		}
	}
}

func (s *subscriptionManager) Unsubscribe(ctx context.Context, topics ...interface{}) error {
	// TODO support a toggle to kill the loop so that when subscription count reaches 0, it exits cleanly
	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	arrw := conn.arrw
	var subType int
	switch topics[0].(type) {
	case RedisPattern:
		subType = subPattern
		arrw.Init(len(topics) + 1).AddString("PUNSUBSCRIBE")
	case string:
		subType = subTopic
		arrw.Init(len(topics) + 1).AddString("UNSUBSCRIBE")
	}
	subs := make([]*subscription, 0, len(topics))
	for _, topic := range topics {
		switch d := topic.(type) {
		case RedisPattern:
			if subType != 0 && subType != subPattern {
				return ErrMixedTopicTypes
			}
			arrw.AddString(string(d))
			if sub, ok := s.chans.Load(string(d)); ok {
				subs = append(subs, sub.(*subscription))
			}
		case string:
			if subType != 0 && subType != subTopic {
				return ErrMixedTopicTypes
			}
			arrw.AddString(d)
			if sub, ok := s.chans.Load(d); ok {
				subs = append(subs, sub.(*subscription))
			}
		default:
			return fmt.Errorf("Wrong type! Expected RedisPattern or string, but received: %T", d)
		}
	}
	err = arrw.Flush()
	if err != nil {
		return err
	}
	for _, sub := range subs {
		s.chans.Delete(sub.topic)
		s.readChan <- sub.topic
	}
	return nil
}
