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

type subMap map[string]*subscription

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
	Topic   string
	Pattern string
	Result  *Result
	Ctx     context.Context
}

type msg struct {
	message *Message
	array   *ArrayResult
}

type subManager struct {
	array     *internalArray
	chans     *sync.Map
	conn      *conn
	msgPool   *sync.Pool
	opts      *PubSubOpts
	writeLock *sync.Mutex
	readChan  chan readFunc
}

func newSubManager(conn *conn, opts *PubSubOpts) *subManager {
	mngr := &subManager{
		array:     &internalArray{arr: &ArrayResult{res: NewResult(conn.buf)}},
		conn:      conn,
		msgPool:   &sync.Pool{},
		writeLock: &sync.Mutex{},
		readChan:  make(chan readFunc),
	}
	mngr.msgPool.New = func() interface{} {
		m := &msg{message: &Message{Result: NewResult(make([]byte, 0, opts.InitBufSize))}}
		m.array.res = m.message.Result
		m.array.buf = m.message.Result.value // this gets relinked in array.Next()
		return m
	}
	return mngr
}

func (s *subManager) tryRead(ctx context.Context, c *conn) (*msg, error) {
	m := s.msgPool.Get().(*msg)
	m.array.r = c.conn
	// Ahh, low-level reading is so much fun
	c.conn.SetReadDeadline(time.Now().Add(s.opts.PingInterval))
	readLen, err := c.conn.Read(m.array.buf[:cap(m.array.buf)])
	m.array.buf = m.array.buf[:readLen]
	// TODO when 1.14 is out, change this to this: https://github.com/golang/go/issues/31449
	// TODO benchmark~
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

func readReply(c *conn, a *internalArray) {
}

// design
// msg from server -> get msg struct from pool -> read into msg buffer -> send msg buffer down to channel ->
// user calls Result.Scan -> msg struct goes back to pool
func (s *subManager) Listen(ctx context.Context, c *conn) {
	s.conn = c
	propList := make([]interface{}, 0, 3)
	for {
		m, err := s.tryRead(ctx, c)
		arr, err := readArray(c.conn, m.array)
		if err != nil {
			var e ErrRetry
			if errors.Is(err, &e) {
				continue // if we are retrying, just skip the loop
			}
			return // otherwise exit
		}
		if arr.Len() == 4 {
			propList = append(propList, &m.message.Pattern)
		}
		propList = append(propList, &m.message.Topic)
		arr.Next() // skip first entry (message type). We already know from len()
		for _, prop := range propList {
			err = arr.Next()
			if err != nil {
				break // TODO log that something went wrong
			}
			err = arr.Scan(prop)
			if err != nil {
				break // TODO log that something went wrong
			}
		}
		// we call next here so that m.array.res (and m.message.Result) contains the final value
		err = arr.Next()
		// reset propList for next iteration
		propList = propList[:0]
		if err == nil {
			s.dispatch(ctx, m)
		}
	}
}

func (s *subManager) getConn() *conn {
	s.writeLock.Lock()
	return s.conn
}

func (s *subManager) putConn(conn *conn) {
	s.conn = conn
	s.writeLock.Unlock()
}

// Design publish command -> publish to channel that we're expecting a read, wait for return on second chan
func (s *subManager) subscribe(ctx context.Context, topics ...interface{}) error {
	subs := make([]*subscription, len(topics))
	cmd := s.conn.cmd
	s.writeLock.Lock()
	var subType int
	for _, topic := range topics {
		var sub *subscription
		switch d := topic.(type) {
		case RedisPattern:
			if subType == 0 {
				subType = subPattern
				cmd = cmd.start(s.conn.buf, len(topics)+1).add("PSUBSCRIBE")
			} else if subType != 0 && subType != subPattern {
				return ErrMixedTopicTypes
			}
			sub = newSubscription(d, s.opts.InitBufSize)
		case string:
			if subType == 0 {
				subType = subTopic
				cmd = cmd.start(s.conn.buf, len(topics)+1).add("SUBSCRIBE")
			} else if subType != 0 && subType != subTopic {
				return ErrMixedTopicTypes
			}
			sub = newSubscription(d, s.opts.InitBufSize)
		default:
			return fmt.Errorf("Wrong type! Expected RedisPattern or string, but received: %T", d)
		}
		cmd.add(sub.topic)
		subs = append(subs, sub)
	}
	var err error
	s.conn.buf, err = cmd.flush()
	s.writeLock.Unlock()
	if err != nil {
		return err
	}
	// TODO the return value here is an array that is the same for psubscribe and subscribe
	// Format is: (action, topic, subscriptionCount)
	// If subscriptionCount drops to 0, we can hand back the connection and kill our poor listener
	// this depends on the configuration of the subscriber portion
	return nil
}

func (s *subManager) dispatch(ctx context.Context, m *msg) {
	m.message.Ctx = ctx
	m.message.Result.finish = func() {
		m.array.reset()
		s.msgPool.Put(m)
		m.message.Result.finish = func() {}
	}
	key := m.message.Topic
	if m.array.Len() == 4 {
		key = m.message.Pattern
	}
	if sub, ok := s.chans.Load(key); ok {
		select {
		case sub.(subscription).msgChan <- m.message:
		default: // TODO log that a message got discarded
		}
	}
}

func (s *subManager) Subscribe(ctx context.Context, sub *subscription) error {
	// For simplicity we only allow one subscriber per pattern (same as Redis), if you want to shoot
	// yourself in the foot you still have that option by using multiple patterns matching the same topic,
	// or registering a meta-callback that can dispatch to all the rest of your evil handlers
	if _, loaded := s.chans.LoadOrStore(sub.topic, sub); loaded {
		return fmt.Errorf("Subscriber already exists for %s", sub.topic)
	}
	return nil
}

func (s *subManager) Unsubscribe(ctx context.Context, topic string) {
	// TODO support a toggle to kill the loop so that when subscription count reaches 0, it exits cleanly
	s.chans.Delete(topic)
}
