package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"runtime/debug"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/dropbox/godropbox/net2"
	goredis "github.com/go-redis/redis"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/mikn/greddis"
	"google.golang.org/grpc/benchmark/stats"
)

func BenchmarkNetSingleBufIO(b *testing.B) {
	debug.SetGCPercent(-1)
	b.ReportAllocs()
	var c, err = net.Dial("tcp", "172.16.28.2:6379")
	var conn = c.(*net.TCPConn)
	if err != nil {
		fmt.Println(err)
		return
	}
	var rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	rw.WriteString("set testkey blahblah\r\n")
	rw.Flush()
	rw.ReadBytes('\n')
	defer conn.Close()
	for i := 0; i < b.N; i++ {
		rw.WriteString("get testkey\r\n")
		rw.Flush()
		rw.ReadBytes('\n')
		rw.ReadBytes('\n')
		//testCall(conn, bytes, ret)
	}
	conn.Write([]byte("del testkey\r\n"))
	debug.FreeOSMemory()
	debug.SetGCPercent(80)
}

func testCallBufIO(rw bufio.ReadWriter) string {
	var b = make([]byte, 10)
	rw.WriteString("get testkey\r\n")
	rw.Flush()
	rw.ReadString('\n')
	//rw.ReadString('\n')
	rw.Read(b)
	//return string(ret[:length])
	return string(b)
}

func BenchmarkSockSingleFunc(b *testing.B) {
	b.ReportAllocs()
	var fd, err = syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	var dst = syscall.SockaddrInet4{
		Port: 6379,
		Addr: [4]byte{172, 17, 0, 2},
	}
	if err != nil {
		panic(err)
	}

	syscall.SetsockoptByte(fd, syscall.SOL_TCP, syscall.TCP_NODELAY, 1)

	syscall.Connect(fd, &dst)
	syscall.Write(fd, []byte("set testkey blahblah\r\n"))
	var bytes = []byte("get testkey\r\n")
	var ret = make([]byte, 0, 14)
	for i := 0; i < b.N; i++ {
		syscall.Write(fd, bytes)
		var ret = ret[:14]
		syscall.Read(fd, ret)
		ret = ret[:]
	}
	syscall.Write(fd, []byte("del testkey\r\n"))
}

func BenchmarkNetSingleFunc(b *testing.B) {
	b.ReportAllocs()
	var conn, err = net.Dial("tcp", "172.16.28.2:6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	//var rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	conn.Write([]byte("set testkey blahblah\r\n"))
	var bytes = make([]byte, 13)
	conn.Read(bytes[:5])
	for i := 0; i < b.N; i++ {
		var str = testCall(conn, bytes)
		if str != "blahblah" {
			panic(fmt.Sprintf("Expected 'blahblah', but got '%s'", str))
		}
	}
	conn.Write([]byte("del testkey\r\n"))
	conn.Read(bytes[:5])
}

func testCall(conn net.Conn, bytes []byte) string {
	bytes = bytes[:0]
	bytes = append(bytes, "get testkey\r\n"...)
	conn.Write(bytes)
	var i = bytes[:4]
	conn.Read(i)
	var length, _ = strconv.ParseInt(string(i[1]), 10, 32)
	bytes = bytes[:length+2]
	conn.Read(bytes)
	var str = string(bytes[:length])
	return str
}

func BenchmarkNetSingle(b *testing.B) {
	b.ReportAllocs()
	var conn, err = net.Dial("tcp", "172.16.28.2:6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	conn.Write([]byte("set testkey blahblah\r\n"))
	var bytes = make([]byte, 0, 13)
	conn.Read(bytes[:5])
	//var bytes = []byte("get testkey\r\n")
	for i := 0; i < b.N; i++ {
		bytes = bytes[:0]
		bytes = append(bytes, "get testkey\r\n"...)
		conn.Write(bytes)
		var i = bytes[:4]
		conn.Read(i)
		var length, _ = strconv.ParseInt(string(i[1]), 10, 32)
		bytes = bytes[:length+2]
		conn.Read(bytes)
	}
	conn.Write([]byte("del testkey\r\n"))
}

func BenchmarkGreddisSingle(b *testing.B) {
	b.ReportAllocs()
	var ctx = context.Background()
	var client = greddis.NewClient(ctx, &greddis.PoolOptions{
		MaxSize: 10,
		Dial: func() (net.Conn, error) {
			return net.Dial("tcp", "172.16.28.2:6379")
		},
	})
	var err = client.Set("testkey", "blahblah", 0)
	if err != nil {
		panic(err)
	}
	var buf = &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		var res, _ = client.Get("testkey")
		res.Scan(buf)
		buf.Reset()
	}
	client.Del("testkey")
}

func BenchmarkGoRedisSingle(b *testing.B) {
	b.ReportAllocs()
	var client = goredis.NewClient(&goredis.Options{
		Addr: "172.16.28.2:6379",
	})
	client.Set("testkey", "blahblah", 0)
	for i := 0; i < b.N; i++ {
		client.Get("testkey").String()
	}
	client.Del("testkey")
}

func BenchmarkRedigoSingle(b *testing.B) {
	b.ReportAllocs()
	var conn, _ = redigo.Dial("tcp", "172.16.28.2:6379")
	conn.Do("set", "testkey", "blahblah")
	for i := 0; i < b.N; i++ {
		var val, err = conn.Do("get", "testkey")
		var _, _ = redigo.String(val, err)
	}
}

func BenchmarkNetChanPool(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = newChanPool("tcp", "172.16.28.2:6379", 10)
	var conn, err = pool.Get()
	if err != nil {
		fmt.Println(err)
		return
	}
	conn.Write([]byte("set testkey blahblah\r\n"))
	var bytes = make([]byte, 0, 13)
	conn.Read(bytes[:5])
	pool.Put(conn)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		conn, _ = pool.Get()
		bytes = bytes[:0]
		bytes = append(bytes, "get testkey\r\n"...)
		conn.Write(bytes)
		var i = bytes[:4]
		conn.Read(i)
		var length, _ = strconv.ParseInt(string(i[1]), 10, 32)
		bytes = bytes[:length+2]
		conn.Read(bytes)
		pool.Put(conn)
		s.Add(time.Now().Sub(t))
	}
	conn, _ = pool.Get()
	conn.Write([]byte("del testkey\r\n"))
	pool.Put(conn)
}

func BenchmarkNetSyncPool(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = newSyncPool("tcp", "172.16.28.2:6379", 10)
	var conn, err = pool.Get()
	if err != nil {
		fmt.Println(err)
		return
	}
	conn.Write([]byte("set testkey blahblah\r\n"))
	var bytes = make([]byte, 0, 13)
	conn.Read(bytes[:5])
	pool.Put(conn)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		conn, _ = pool.Get()
		bytes = bytes[:0]
		bytes = append(bytes, "get testkey\r\n"...)
		conn.Write(bytes)
		var i = bytes[:4]
		conn.Read(i)
		var length, _ = strconv.ParseInt(string(i[1]), 10, 32)
		bytes = bytes[:length+2]
		conn.Read(bytes)
		pool.Put(conn)
		s.Add(time.Now().Sub(t))
	}
	conn, _ = pool.Get()
	conn.Write([]byte("del testkey\r\n"))
	pool.Put(conn)
}

func BenchmarkNetAtomicPool(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = newAtomicPool("tcp", "172.16.28.2:6379", 10)
	var conn, err = pool.Get()
	if err != nil {
		fmt.Println(err)
		return
	}
	conn.Write([]byte("set testkey blahblah\r\n"))
	var bytes = make([]byte, 0, 13)
	conn.Read(bytes[:5])
	pool.Put(conn)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		conn, _ = pool.Get()
		bytes = bytes[:0]
		bytes = append(bytes, "get testkey\r\n"...)
		conn.Write(bytes)
		var i = bytes[:4]
		conn.Read(i)
		var length, _ = strconv.ParseInt(string(i[1]), 10, 32)
		bytes = bytes[:length+2]
		conn.Read(bytes)
		pool.Put(conn)
		s.Add(time.Now().Sub(t))
	}
	conn, _ = pool.Get()
	conn.Write([]byte("del testkey\r\n"))
	pool.Put(conn)
}

func BenchmarkNetSemPool(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = newSemPool("tcp", "172.16.28.2:6379", 10)
	var conn, err = pool.Get()
	if err != nil {
		fmt.Println(err)
		return
	}
	conn.Write([]byte("set testkey blahblah\r\n"))
	var bytes = make([]byte, 0, 13)
	conn.Read(bytes[:5])
	pool.Put(conn)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		conn, _ = pool.Get()
		bytes = bytes[:0]
		bytes = append(bytes, "get testkey\r\n"...)
		conn.Write(bytes)
		var i = bytes[:4]
		conn.Read(i)
		var length, _ = strconv.ParseInt(string(i[1]), 10, 32)
		bytes = bytes[:length+2]
		conn.Read(bytes)
		pool.Put(conn)
		s.Add(time.Now().Sub(t))
	}
	conn, _ = pool.Get()
	conn.Write([]byte("del testkey\r\n"))
	pool.Put(conn)
}

func BenchmarkNet2Pool(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = net2.NewSimpleConnectionPool(net2.ConnectionOptions{
		MaxActiveConnections: 10,
		MaxIdleConnections:   10,
	})
	pool.Register("tcp", "172.16.28.2:6379")
	var conn, err = pool.Get("tcp", "172.16.28.2:6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	conn.Write([]byte("set testkey blahblah\r\n"))
	var bytes = make([]byte, 0, 13)
	conn.Read(bytes[:5])
	conn.ReleaseConnection()
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		conn, _ = pool.Get("tcp", "172.16.28.2:6379")
		bytes = bytes[:0]
		bytes = append(bytes, "get testkey\r\n"...)
		conn.Write(bytes)
		var i = bytes[:4]
		conn.Read(i)
		var length, _ = strconv.ParseInt(string(i[1]), 10, 32)
		bytes = bytes[:length+2]
		conn.Read(bytes)
		conn.ReleaseConnection()
		s.Add(time.Now().Sub(t))
	}
	conn, _ = pool.Get("tcp", "172.16.28.2:6379")
	conn.Write([]byte("del testkey\r\n"))
	conn.ReleaseConnection()
}

func BenchmarkGreddisGet8b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var ctx = context.Background()
	var client = greddis.NewClient(ctx, &greddis.PoolOptions{
		MaxSize: 10,
		Dial: func() (net.Conn, error) {
			return net.Dial("tcp", "172.16.28.2:6379")
		},
	})
	client.Set("testkey", "blahblah", 0)
	var buf = &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		var res, err = client.Get("testkey")
		if err != nil {
			fmt.Println(err.Error())
		}
		res.Scan(buf)
		buf.Reset()
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkGoRedisGet8b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var client = goredis.NewClient(&goredis.Options{
		Addr:     "172.16.28.2:6379",
		PoolSize: 10,
	})
	client.Set("testkey", "blahblah", 0)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		client.Get("testkey").String()
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkRedigoGet8b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = redigo.Pool{
		MaxIdle:   10,
		MaxActive: 10,
		Dial:      func() (redigo.Conn, error) { return redigo.Dial("tcp", "172.16.28.2:6379") },
	}
	var conn = pool.Get()
	conn.Do("set", "testkey", "blahblah")
	conn.Close()
	var buf = make([][]byte, 1)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		conn = pool.Get()
		var val, _ = redigo.Values(conn.Do("get", "testkey"))
		redigo.Scan(val, &buf)
		conn.Close()
		s.Add(time.Now().Sub(t))
		buf[0] = buf[0][:0]
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func BenchmarkGreddisSet8b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var ctx = context.Background()
	var client = greddis.NewClient(ctx, &greddis.PoolOptions{
		MaxSize: 10,
		Dial: func() (net.Conn, error) {
			return net.Dial("tcp", "172.16.28.2:6379")
		},
	})
	var randStr = RandStringBytes(8)
	var strPtr = &randStr
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		client.Set("testkey", strPtr, 0)
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkGoRedisSet8b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var client = goredis.NewClient(&goredis.Options{
		Addr:     "172.16.28.2:6379",
		PoolSize: 10,
	})
	var randStr = RandStringBytes(8)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		client.Set("testkey", randStr, 0)
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkRedigoSet8b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = redigo.Pool{
		MaxIdle:   10,
		MaxActive: 10,
		Dial:      func() (redigo.Conn, error) { return redigo.Dial("tcp", "172.16.28.2:6379") },
	}
	var randStr = RandStringBytes(8)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		var conn = pool.Get()
		conn.Do("set", "testkey", randStr)
		conn.Close()
		s.Add(time.Now().Sub(t))
	}
}

func BenchmarkGreddisGet5000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var ctx = context.Background()
	var client = greddis.NewClient(ctx, &greddis.PoolOptions{
		MaxSize: 10,
		Dial: func() (net.Conn, error) {
			return net.Dial("tcp", "172.16.28.2:6379")
		},
	})
	var str = RandStringBytes(5000)
	client.Set("testkey", str, 0)
	var buf = &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		var res, _ = client.Get("testkey")
		res.Scan(buf)
		s.Add(time.Now().Sub(t))
		buf.Reset()
	}
	client.Del("testkey")
}

func BenchmarkGoRedisGet5000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var client = goredis.NewClient(&goredis.Options{
		Addr:     "172.16.28.2:6379",
		PoolSize: 10,
	})
	client.Set("testkey", RandStringBytes(5000), 0)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		client.Get("testkey").String()
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkRedigoGet5000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = redigo.Pool{
		MaxIdle:   10,
		MaxActive: 10,
		Dial:      func() (redigo.Conn, error) { return redigo.Dial("tcp", "172.16.28.2:6379") },
	}
	var conn = pool.Get()
	conn.Do("set", "testkey", RandStringBytes(5000))
	conn.Close()
	var buf = make([][]byte, 1)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		conn = pool.Get()
		var val, _ = redigo.Values(conn.Do("get", "testkey"))
		redigo.Scan(val, &buf)
		conn.Close()
		s.Add(time.Now().Sub(t))
		buf[0] = buf[0][:0]
	}
}

func BenchmarkGreddisSet5000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var ctx = context.Background()
	var client = greddis.NewClient(ctx, &greddis.PoolOptions{
		MaxSize: 10,
		Dial: func() (net.Conn, error) {
			return net.Dial("tcp", "172.16.28.2:6379")
		},
	})
	var randStr = RandStringBytes(5000)
	var strPtr = &randStr
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		client.Set("testkey", strPtr, 0)
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkGoRedisSet5000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var client = goredis.NewClient(&goredis.Options{
		Addr:     "172.16.28.2:6379",
		PoolSize: 10,
	})
	var randStr = RandStringBytes(5000)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		client.Set("testkey", randStr, 0)
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkRedigoSet5000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = redigo.Pool{
		MaxIdle:   10,
		MaxActive: 10,
		Dial:      func() (redigo.Conn, error) { return redigo.Dial("tcp", "172.16.28.2:6379") },
	}
	var randStr = RandStringBytes(5000)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		var conn = pool.Get()
		conn.Do("set", "testkey", randStr)
		conn.Close()
		s.Add(time.Now().Sub(t))
	}
}

func BenchmarkGreddisGet50000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var ctx = context.Background()
	var client = greddis.NewClient(ctx, &greddis.PoolOptions{
		MaxSize: 10,
		Dial: func() (net.Conn, error) {
			return net.Dial("tcp", "172.16.28.2:6379")
		},
	})
	var str = RandStringBytes(50000)
	client.Set("testkey", str, 0)
	var buf = &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		var res, err = client.Get("testkey")
		if err != nil {
			fmt.Println(err)
			break
		}
		res.Scan(buf)
		s.Add(time.Now().Sub(t))
		buf.Reset()
	}
	client.Del("testkey")
}

func BenchmarkGoRedisGet50000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var client = goredis.NewClient(&goredis.Options{
		Addr:     "172.16.28.2:6379",
		PoolSize: 10,
	})
	client.Set("testkey", RandStringBytes(50000), 0)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		client.Get("testkey").String()
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkRedigoGet50000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = redigo.Pool{
		MaxIdle:   10,
		MaxActive: 10,
		Dial:      func() (redigo.Conn, error) { return redigo.Dial("tcp", "172.16.28.2:6379") },
	}
	var conn = pool.Get()
	conn.Do("set", "testkey", RandStringBytes(50000))
	conn.Close()
	var buf = make([][]byte, 1)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		conn = pool.Get()
		var val, _ = redigo.Values(conn.Do("get", "testkey"))
		redigo.Scan(val, &buf)
		conn.Close()
		s.Add(time.Now().Sub(t))
		buf[0] = buf[0][:0]
	}
}

func BenchmarkGreddisSet50000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var ctx = context.Background()
	var client = greddis.NewClient(ctx, &greddis.PoolOptions{
		MaxSize: 10,
		Dial: func() (net.Conn, error) {
			return net.Dial("tcp", "172.16.28.2:6379")
		},
	})
	var randStr = RandStringBytes(50000)
	var strPtr = &randStr
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		client.Set("testkey", strPtr, 0)
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkGoRedisSet50000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var client = goredis.NewClient(&goredis.Options{
		Addr:     "172.16.28.2:6379",
		PoolSize: 10,
	})
	var randStr = RandStringBytes(50000)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		client.Set("testkey", randStr, 0)
		s.Add(time.Now().Sub(t))
	}
	client.Del("testkey")
}

func BenchmarkRedigoSet50000b(b *testing.B) {
	b.ReportAllocs()
	var s = stats.AddStats(b, 10)
	var pool = redigo.Pool{
		MaxIdle:   10,
		MaxActive: 10,
		Dial:      func() (redigo.Conn, error) { return redigo.Dial("tcp", "172.16.28.2:6379") },
	}
	var randStr = RandStringBytes(50000)
	for i := 0; i < b.N; i++ {
		var t = time.Now()
		var conn = pool.Get()
		conn.Do("set", "testkey", randStr)
		conn.Close()
		s.Add(time.Now().Sub(t))
	}
}

//func TestMain(m *testing.M) {
//	var code = stats.RunTestMain(m)
//	os.Exit(code)
//}
