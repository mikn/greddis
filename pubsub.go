package greddis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// TODO do multi-core benchmarks to ensure it scales ok

const (
	subTopic = iota + 1
	subPattern
)

type MessageChan <-chan *Message

type MessageChanMap map[string]MessageChan

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

type subscription struct {
	topic   string
	msgChan chan *Message
	subType int
}

type Message struct {
	Ctx     context.Context
	Pattern string
	Result  *Result
	Topic   string
	array   *ArrayResult
}

type msgPool struct {
	msgs        chan *Message
	initBufSize int
	entityId    int
}

func newMsgPool(initBufSize int) *msgPool {
	return &msgPool{
		initBufSize: initBufSize,
		msgs:        make(chan *Message, 10),
	}
}

func (mp *msgPool) Get() *Message {
	var m *Message
	select {
	case m = <-mp.msgs:
	default:
		m = mp.New()
	}
	return m
}

func (mp *msgPool) New() *Message {
	res := NewResult(make([]byte, 0, mp.initBufSize))
	m := &Message{
		Result: res,
		array:  &ArrayResult{res: res, buf: res.value},
	}
	res.finish = func() {
		m.array.reset()
		mp.Put(m)
	}
	return m
}

func (mp *msgPool) Put(m *Message) {
	select {
	case mp.msgs <- m:
	default:
	}
}

func newSubManager(pool internalPool, opts *PubSubOpts) *subManager {
	mngr := &subManager{
		opts:      opts,
		array:     &internalArray{arr: &ArrayResult{res: NewResult(nil)}},
		chans:     &sync.Map{},
		connPool:  pool,
		msgPool:   newMsgPool(opts.InitBufSize),
		writeLock: &sync.Mutex{},
		readChan:  make(chan string, 10),
		listening: false,
	}
	return mngr
}

type subManager struct {
	listening bool
	array     *internalArray
	chans     *sync.Map
	conn      *conn
	connPool  internalPool
	msgPool   *msgPool
	opts      *PubSubOpts
	writeLock *sync.Mutex
	readChan  chan string
}

func (s *subManager) getConn(ctx context.Context) (*conn, error) {
	var err error
	if s.conn == nil {
		s.conn, err = s.connPool.Get(ctx)
	}
	return s.conn, err
}

func (s *subManager) tryRead(ctx context.Context, c *conn) (*Message, error) {
	m := s.msgPool.Get()
	m.array.r = c.conn
	// Ahh, low-level reading is so much fun
	c.conn.SetReadDeadline(time.Now().Add(s.opts.PingInterval))
	readLen, err := c.conn.Read(m.array.buf[:cap(m.array.buf)])
	m.array.buf = m.array.buf[:readLen]
	// TODO when 1.14 is out, change this to this: https://github.com/golang/go/issues/31449
	if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
		c.conn.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout))
		err = ping(ctx, c)
		if err != nil {
			// TODO Log here that the subscriber has stopped listening
			return nil, err
		}
		return nil, ErrRetry{ErrTimeout}
	}
	if readLen == 0 {
		return nil, ErrRetry{ErrNoData} // TODO we should probably stop reading after a while here
	}
	return m, nil
}

func (s *subManager) Listen(ctx context.Context, c *conn) {
	s.conn = c
	var count int
	for {
		m, err := s.tryRead(ctx, c)
		if err != nil {
			var e ErrRetry
			if errors.Is(err, &e) {
				continue // if we are retrying, just skip the loop
			}
			return // otherwise exit
		}
		m.array, err = readArray(c.conn, m.array)
		if err != nil {
			fmt.Println(err)
			return
		}
		s.array.arr = m.array
		switch s.array.SwitchOnNext() {
		case "pmessage":
			s.array.Next().Scan(&m.Pattern, &m.Topic)
		case "message":
			// TODO This causes an allocation because m.Topic is a string, which triggers a copy
			s.array.Next().Scan(&m.Topic)
		case "subscribe":
			s.array.Next().Expect(<-s.readChan).Scan(&count)
		case "psubscribe":
			s.array.Next().Expect(<-s.readChan).Scan(&count)
		case "unsubscribe":
		case "punsubscribe":
		default:
			fmt.Printf("We shouldn't be here :( value: '%s'\n", string(s.array.arr.res.value))
		}
		err = s.array.GetErrors()
		if err != nil {
			fmt.Printf("count '%d'\n", count)
			fmt.Printf("error: %T\n", err)
			// TODO log errors here
			continue
		}
		// we call next here so that m.array.res (and m.Result) contains the final value
		m.array.Next()
		s.dispatch(ctx, m)
	}
}

// Design publish command -> publish to channel that we're expecting a read, wait for return on second chan
func (s *subManager) Subscribe(ctx context.Context, topics ...interface{}) (MessageChanMap, error) {
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
	cmd := s.conn.cmd
	var subType int
	for i, topic := range topics {
		var sub *subscription
		switch d := topic.(type) {
		case RedisPattern:
			if subType == 0 {
				subType = subPattern
				cmd = cmd.start(s.conn.buf, len(topics)+1).add("PSUBSCRIBE")
			} else if subType != 0 && subType != subPattern {
				return nil, ErrMixedTopicTypes
			}
			sub = newSubscription(d, s.opts.InitBufSize)
		case string:
			if subType == 0 {
				subType = subTopic
				cmd = cmd.start(s.conn.buf, len(topics)+1).add("SUBSCRIBE")
			} else if subType != 0 && subType != subTopic {
				return nil, ErrMixedTopicTypes
			}
			sub = newSubscription(d, s.opts.InitBufSize)
		default:
			return nil, fmt.Errorf("Wrong type! Expected RedisPattern or string, but received: %T", d)
		}
		s.readChan <- sub.topic
		cmd.add(sub.topic)
		subs[i] = sub
		chanMap[sub.topic] = sub.msgChan
	}
	var err error
	s.conn.buf, err = cmd.flush()
	s.writeLock.Unlock()
	if err != nil {
		return nil, err
	}
	// Need to store the subscriptions after we flush the command so we only store valid subscriptions
	for _, sub := range subs {
		s.chans.Store(sub.topic, sub)
	}
	// TODO If subscriptionCount drops to 0, we can hand back the connection and kill our poor listener
	// this depends on the configuration of the subscriber portion
	return chanMap, nil
}

func (s *subManager) dispatch(ctx context.Context, m *Message) {
	m.Ctx = ctx
	key := m.Topic
	if m.array.Len() == 4 {
		key = m.Pattern
	}
	if sub, ok := s.chans.Load(key); ok {
		select {
		case sub.(*subscription).msgChan <- m:
		default: // TODO log that a message got discarded
		}
	}
}

func (s *subManager) Unsubscribe(ctx context.Context, topics ...interface{}) error {
	// TODO support a toggle to kill the loop so that when subscription count reaches 0, it exits cleanly
	conn, err := s.getConn(ctx)
	if err != nil {
		return err
	}
	c := conn.cmd
	var subType int
	for _, topic := range topics {
		switch d := topic.(type) {
		case RedisPattern:
			if subType == 0 {
				subType = subPattern
				c = c.start(c.buf, len(topics)+1).add("PUNSUBSCRIBE")
			} else if subType != 0 && subType != subPattern {
				return ErrMixedTopicTypes
			}
			c.add(string(d))
			s.chans.Delete(string(d))
		case string:
			if subType == 0 {
				subType = subTopic
				c = c.start(c.buf, len(topics)+1).add("UNSUBSCRIBE")
			} else if subType != 0 && subType != subTopic {
				return ErrMixedTopicTypes
			}
			c.add(d)
			s.chans.Delete(d)
		default:
			return fmt.Errorf("Wrong type! Expected RedisPattern or string, but received: %T", d)
		}
	}
	c.buf, err = c.flush()
	if err != nil {
		return err
	}
	return nil
}
