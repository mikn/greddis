package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/semaphore"
)

func main() {
	var conn, err = net.Dial("tcp", "172.17.0.2:6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	//var rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	conn.Write([]byte("set testkey blahblah\n"))
	var rw = bufio.NewReader(conn)
	var b = bytes.Buffer{}
	rw.ReadString('\n')
	defer conn.Close()
	b.WriteString("get testkey\n")
	b.WriteTo(conn)
	var r, _, _ = rw.ReadLine()
	fmt.Println(string(r))
	conn.Write([]byte("del testkey\n"))
}

type chanPool struct {
	conns   chan net.Conn
	network string
	address string
	maxSize int
}

func newChanPool(network string, address string, maxSize int) *chanPool {
	var pool = &chanPool{
		conns:   make(chan net.Conn, maxSize),
		network: network,
		address: address,
		maxSize: maxSize,
	}
	return pool
}

func (p *chanPool) Get() (c net.Conn, err error) {
	err = nil
	select {
	case c = <-p.conns:
	default:
		c, err = net.Dial(p.network, p.address)
	}
	return c, err
}

func (p *chanPool) Put(c net.Conn) {
	select {
	case p.conns <- c:
	default:
	}
}

type semPool struct {
	conns   []net.Conn
	ctx     context.Context
	mutex   sync.Mutex
	sem     *semaphore.Weighted
	maxSize int
	network string
	address string
}

func newSemPool(network string, address string, maxSize int64) *semPool {
	return &semPool{
		conns:   make([]net.Conn, 0, maxSize),
		ctx:     context.Background(),
		mutex:   sync.Mutex{},
		sem:     semaphore.NewWeighted(maxSize),
		maxSize: int(maxSize),
		network: network,
		address: address,
	}
}

func (s *semPool) Get() (net.Conn, error) {
	s.sem.Acquire(s.ctx, 1)
	s.mutex.Lock()
	var conn net.Conn
	var err error
	if len(s.conns) == 0 {
		conn, err = net.Dial(s.network, s.address)
		if err != nil {
			s.sem.Release(1)
			s.mutex.Unlock()
			fmt.Println("zomg something went wrong")
			return nil, err
		}
		s.conns = append(s.conns, conn)
	} else {
		conn = s.conns[0]
		s.conns = s.conns[1:]
	}
	s.mutex.Unlock()
	return conn, err
}

func (s *semPool) Put(conn net.Conn) {
	if len(s.conns) > s.maxSize {
		fmt.Println("omg, why put more conns here!?")
		return
	}
	s.mutex.Lock()
	s.conns = append(s.conns, conn)
	s.mutex.Unlock()
	s.sem.Release(1)
}

type syncPool struct {
	conns sync.Pool
	sem   *semaphore.Weighted
	ctx   context.Context
}

type poolRet struct {
	Conn net.Conn
	Err  error
}

func newSyncPool(network string, address string, maxSize int64) *syncPool {
	var pool = syncPool{
		conns: sync.Pool{
			New: func() interface{} {
				var conn, err = net.Dial(network, address)
				return poolRet{conn, err}
			},
		},
		sem: semaphore.NewWeighted(maxSize),
		ctx: context.Background(),
	}
	return &pool
}

func (s *syncPool) Get() (net.Conn, error) {
	s.sem.Acquire(s.ctx, 1)
	var ret = s.conns.Get().(poolRet)
	if ret.Err != nil {
		s.sem.Release(1)
		return nil, ret.Err
	}
	return ret.Conn, ret.Err
}

func (s *syncPool) Put(conn net.Conn) {
	s.conns.Put(poolRet{conn, nil})
	s.sem.Release(1)
}

type atomicPool struct {
	conns   []net.Conn
	mutex   sync.Mutex
	counter int32
	maxSize int
	network string
	address string
}

func newAtomicPool(network string, address string, maxSize int64) *atomicPool {
	var pool = atomicPool{
		conns:   make([]net.Conn, 0, maxSize),
		mutex:   sync.Mutex{},
		maxSize: int(maxSize),
		network: network,
		address: address,
	}
	atomic.StoreInt32(&pool.counter, int32(maxSize))
	return &pool
}

func (s *atomicPool) Get() (net.Conn, error) {
	if atomic.LoadInt32(&s.counter) > 0 {
		atomic.AddInt32(&s.counter, -1)
	} else {
		fmt.Println("OMG RAN OUT OF CONNECTIONS!")
		return nil, errors.New("omgomgomg")
	}
	s.mutex.Lock()
	var conn net.Conn
	var err error
	if len(s.conns) == 0 {
		conn, err = net.Dial(s.network, s.address)
		if err != nil {
			s.mutex.Unlock()
			atomic.AddInt32(&s.counter, 1)
			fmt.Println("zomg something went wrong")
			return nil, err
		}
		s.conns = append(s.conns, conn)
	} else {
		conn = s.conns[0]
		s.conns = s.conns[1:]
	}
	s.mutex.Unlock()
	return conn, err
}

func (s *atomicPool) Put(conn net.Conn) {
	if len(s.conns) > s.maxSize {
		fmt.Println("omg, why put more conns here!?")
		return
	}
	s.mutex.Lock()
	s.conns = append(s.conns, conn)
	s.mutex.Unlock()
	atomic.AddInt32(&s.counter, 1)
}
