package greddis

import (
	"context"
	"errors"
	"fmt"
	"io"
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

type Message struct {
	Ctx     context.Context
	Pattern string
	Result  *Result
	Topic   string
	array   *ArrayReader
}

type MessageChan <-chan *Message

type MessageChanMap map[string]MessageChan

type msgPool struct {
	initBufSize int
	msgs        sync.Pool
}

func newMsgPool(initBufSize int) *msgPool {
	mp := &msgPool{
		initBufSize: initBufSize,
		msgs:        sync.Pool{},
	}
	mp.msgs.New = func() interface{} {
		res := NewResult(make([]byte, 0, mp.initBufSize))
		m := &Message{
			Result: res,
			array:  &ArrayReader{res: res, buf: res.value},
		}
		res.finish = func() {
			m.array.reset()
			mp.Put(m)
		}
		return m
	}
	return mp
}

func (mp *msgPool) Get(r io.Reader) *Message {
	msg := mp.msgs.Get().(*Message)
	msg.array.ResetReader(r)
	return msg
}

func (mp *msgPool) Put(m *Message) {
	mp.msgs.Put(m)
}

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
	msgPool   *msgPool
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
		msgPool:   newMsgPool(opts.InitBufSize),
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
func (s *subscriptionManager) tryRead(ctx context.Context, buf []byte, c *conn) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Ahh, low-level reading is so much fun. Here we're waiting for a new message
		c.conn.SetReadDeadline(time.Now().Add(s.opts.PingInterval))
		readLen, err := c.conn.Read(buf[:cap(buf)])
		buf = buf[:readLen]
		// TODO when 1.15 is out, change this to this: https://github.com/golang/go/issues/31449
		if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
			// every time we time out, we send a ping to make sure the connection is still alive
			c.conn.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout))
			err = ping(ctx, c)
			if err != nil {
				return buf, err
			}
			return buf, fmt.Errorf("Timed out whilst waiting for data: %w", ErrRetryable)
		} else if err != nil {
			return nil, err
		}
		if readLen == 0 {
			return buf, fmt.Errorf("No data was read: %w", ErrRetryable) // TODO we should probably stop reading after a while here
		}
		return buf, nil
	}
}

func (s *subscriptionManager) Listen(ctx context.Context, c *conn) {
	s.conn = c
	m := s.msgPool.Get(c.conn)
	var count int
	var errs []error
	var err error
	for {
		select {
		case <-ctx.Done():
			log.Printf("%s\n", ctx.Err())
			return
		default:
			m.array.buf, err = s.tryRead(ctx, m.array.buf, c)
			if err != nil {
				if errors.Is(err, ErrRetryable) {
					continue // if we are retrying, just skip the loop
				}
				return // TODO Log here that the subscriber has stopped listening
			}
			// we fill in the rest of the array here (no we don't - this function does not read the members)
			m.array, err = readArray(c.conn, m.array)
			if err != nil {
				log.Printf("%s\n", err)
				return
			}
			arrTopicCount := func() int { return int(math.Ceil(float64(m.array.Len())/2) - 1) }
			switch m.array.SwitchOnNext() {
			case "pmessage":
				m.array.Scan(&m.Pattern, &m.Topic)
			case "message":
				// TODO This causes an allocation because m.Topic is a string, which triggers a copy
				// proposal: make it m.Topic() instead which returns a string from the []byte of m.topic
				m.array.Scan(&m.Topic)
			case "psubscribe":
				for i := 0; i < arrTopicCount(); i++ {
					if err := m.array.Expect(<-s.readChan); err != nil {
						errs = append(errs, err)
					}
					if err := m.array.Scan(&count); err != nil {
						errs = append(errs, err)
					}
				}
			case "subscribe":
				for i := 0; i < arrTopicCount(); i++ {
					if err := m.array.Expect(<-s.readChan); err != nil {
						errs = append(errs, err)
					}
					if err := m.array.Scan(&count); err != nil {
						errs = append(errs, err)
					}
				}
			case "punsubscribe":
			case "unsubscribe":
			default:
				log.Printf("We shouldn't be here :( value: '%s'\n", string(m.array.res.value))
			}
			for i, err := range errs {
				log.Printf("Error %d - %s", i, err)
			}
			if m.array.Len() > m.array.pos {
				s.dispatch(ctx, m)
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
	if m.array.Len() == 4 {
		key = m.Pattern
	}
	m.array.prepare()
	if sub, ok := s.chans.Load(key); ok {
		select {
		case <-ctx.Done():
			return
		case sub.(*subscription).msgChan <- m:
		}
	} else {
		log.Printf("no subsciption for topic: %s", key)
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
