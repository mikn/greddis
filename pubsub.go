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
	Pattern string
	Result  *Result
	Topic   string
}

func newMessage(res *Result, msgChan chan *Message) *Message {
	msg := &Message{
		Result: res,
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
		msgChan:   make(chan *Message, 1),
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

// tryRead - wait for a message to arrive
// parseMessage - read message into Struct
func (s *subscriptionManager) tryRead(ctx context.Context, c *conn, array *ArrayReader) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Ahh, low-level reading is so much fun. Here we're waiting for a new message
		c.conn.SetReadDeadline(time.Now().Add(s.opts.PingInterval))

		err := array.Init()
		// TODO when 1.15 is out, change this to this: https://github.com/golang/go/issues/31449
		if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
			// every time we time out, we send a ping to make sure the connection is still alive
			c.conn.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout))
			err = ping(ctx, c)
			if err != nil {
				return err
			}
			return fmt.Errorf("Timed out whilst waiting for data: %w", ErrRetryable)
		} else if err != nil {
			return err
		}
		return nil
	}
}

func (s *subscriptionManager) Listen(ctx context.Context, c *conn) {
	array := NewArrayReader(c.r)
	var count int
	var errs []error
	var err error
	for {
		select {
		case <-ctx.Done():
			log.Printf("%s\n", ctx.Err())
			return
		case m := <-s.msgChan:
			if m == nil {
				return
			}
			err = s.tryRead(ctx, c, array)
			if err != nil {
				if errors.Is(err, ErrRetryable) {
					continue // if we are retrying, just skip the loop
				}
				log.Println(err)
				return // TODO Log here that the subscriber has stopped listening
			}
			arrTopicCount := func() int { return int(math.Ceil(float64(array.Len())/2) - 1) }
			switch array.Next(ScanBulkString).SwitchOnNext() {
			case "pmessage":
				array.Scan(&m.Pattern, &m.Topic)
			case "message":
				// TODO This causes an allocation because m.Topic is a string, which triggers a copy
				// proposal: make it m.Topic() instead which returns a string from the []byte of m.topic
				array.Scan(&m.Topic)
			case "psubscribe":
				for i := 0; i < arrTopicCount(); i++ {
					if err := array.Expect(<-s.readChan); err != nil {
						errs = append(errs, err)
					}
					if err := array.Next(ScanInteger).Scan(&count); err != nil {
						errs = append(errs, err)
					}
				}
			case "subscribe":
				for i := 0; i < arrTopicCount(); i++ {
					if err := array.Expect(<-s.readChan); err != nil {
						errs = append(errs, err)
					}
					if err := array.Next(ScanInteger).Scan(&count); err != nil {
						errs = append(errs, err)
					}
				}
			case "punsubscribe":
			case "unsubscribe":
			default:
				v, _ := array.r.r.Peek(array.r.r.Buffered())
				log.Printf("We shouldn't be here :( value: '%s' tokenLen: %d arrayLen: %d scanFuncLen: %d buffered: %d\nContents: %s", array.r.String(), array.r.Len(), array.Len(), array.scanFuncs.Len(), array.r.r.Buffered(), v)
			}
			for i, err := range errs {
				log.Printf("Error %d - %s", i, err)
			}
			if array.Len() > array.pos {
				err := array.next()
				if err != nil {
					log.Printf("Next error: %s\n", err)
				}
				s.dispatch(ctx, m)
			} else {
				s.msgChan <- m
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
		s.msgChan <- newMessage(NewResult(conn.r), s.msgChan)
		go s.Listen(ctx, conn)
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
				return nil, ErrMixedTopicTypes
			}
			sub = newSubscription(d, s.opts.InitBufSize)
			sub.subType = subPattern
			sub.topic = string(d)
		case string:
			if subType != subTopic {
				return nil, ErrMixedTopicTypes
			}
			sub = newSubscription(d, s.opts.InitBufSize)
			sub.subType = subPattern
			sub.topic = string(d)
		default:
			return nil, fmt.Errorf("Wrong type! Expected RedisPattern or string, but received: %T", d)
		}
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
	key := m.Topic
	if m.Pattern != "" {
		key = m.Pattern
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
	for _, topic := range topics {
		switch d := topic.(type) {
		case RedisPattern:
			if subType == 0 {
				subType = subPattern
				arrw.Init(len(topics)+1).AddString("PUNSUBSCRIBE", string(d))
			} else if subType != 0 && subType != subPattern {
				return ErrMixedTopicTypes
			}
			s.chans.Delete(string(d))
		case string:
			if subType == 0 {
				subType = subTopic
				arrw.Init(len(topics)+1).AddString("UNSUBSCRIBE", d)
			} else if subType != 0 && subType != subTopic {
				return ErrMixedTopicTypes
			}
			s.chans.Delete(d)
		default:
			return fmt.Errorf("Wrong type! Expected RedisPattern or string, but received: %T", d)
		}
	}
	err = arrw.Flush()
	if err != nil {
		return err
	}
	return nil
}
