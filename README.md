# Greddis

[![CircleCI](https://circleci.com/gh/mikn/greddis/tree/master.svg?style=svg)](https://circleci.com/gh/mikn/greddis/tree/master)
[![codecov](https://codecov.io/gh/mikn/greddis/branch/master/graph/badge.svg)](https://codecov.io/gh/mikn/greddis)
[![Go Report Card](https://goreportcard.com/badge/github.com/mikn/greddis)](https://goreportcard.com/report/github.com/mikn/greddis)
[![Go doc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/mikn/greddis)

**Note**: Currently Greddis only implements Del/Set/Get and is more fo a proof of concept than a fully implemented client.

Greddis focus is high performance and letting the user of the library control its allocations. Is built using learnings from `database/sql/driver` implementations in the standard package. Many implementations of these interfaces provide consistent and high performance through good use of buffer pools, as well as the connection pool implementation in the standard library itself.

Furthermore, it is compatible with any implementation of Valuer/Scanner from `database/sql` as long as they have a `[]byte` implementation (as all data returned from Redis is `[]byte`).

## How does it compare to Redigo/GoRedis?

### Efficient use of connections?

| Client  |     |
| ------- | --- |
| Greddis | Yes |
| Redigo  | No  |
| GoRedis | Yes |

**Redigo** returns the underlying connection to execute your commands against (given Go's concurrency model, this gives you an insignificant performance benefit), whilst **Greddis** and **GoRedis** relies on a higher-level abstraction called "client" which is closer to the `database/sql/driver` abstraction. This means that between each interaction with the redis client the connection is being put back onto the pool. In high concurrency situations this is preferrable, as holding on to the connection across calls means your connection pool will grow faster than if you were to return it immediately (again see `database/sql` pool implementation).

### Implements Redis Client protocol?

| Client  |     |
| ------- | --- |
| Greddis | Yes |
| Redigo  | No  |
| GoRedis | No  |

According to the Redis Serialization Protocol ([RESP Specification](https://redis.io/topics/protocol)), client libraries should use the RESP protocol to make requests as well as parse it for responses. The other option is to use their "human readable" Telnet protocol, which both **GoRedis** and **Redigo** implements. The problem is that this does not allow the Redis server to up-front malloc the entire memory section required to store the request before parsing it, and thus it needs to iteratively parse the return in chunks until it reaches the end. This explains why **Greddis's** performance on Set is barely related to the payload size.

### Pools request and response buffers to amortize allocation cost?

| Client  |     |
| ------- | --- |
| Greddis | Yes |
| Redigo  | No  |
| GoRedis | No  |

Neither **Redigo** nor **GoRedis** pools its buffers for reuse and allocates them on the stack per request. This becomes rather heavy on performance, especially as response sizes grow. You can see in the Get benchmarks for the three clients that **GoRedis** allocates the result no less than three times on the stack and **Redigo** allocates once. (Reference, *Get5000b* benchmark)

### Allows for zero-copy parsing of response?

| Client  |                  |
| ------- | ---------------- |
| Greddis | Yes              |
| Redigo  | No (but kind of) |
| GoRedis | No               |

The `Result.Scan` interface provided by the `database/sql` package is designed to allow you to do zero-alloc/zero-copy result parsing. **GoRedis** does not have a scan command at all. And **Redigo**, whilst having a `Scan` command, still does one copy per response before passing it to `Scan`. It also uses reflection on the type you send in to ensure it is of the same type as what's been parsed, rather than sending in the raw `[]byte` slice for casting (what `database/sql` does). **Greddis** has opted to implement the `Result.Scan` interface more closely and only supports the `Result.Scan` interface for responses, allowing the user to control the casting and parsing depending on type sent in to `Result.Scan`.

## Benchmarks

The benchmarks are run against a real redis server on localhost (network stack), so no mock and no direct socket connection.

If we can maintain a single connection, how fast can we go?
Also note the SockSingleFunc benchmark is implemented using syscalls, so it blocks the entire go-routine thread rather than using epoll whilst waiting for the response, so it is not realistic to use.
```
BenchmarkNetSingleBufIO-8     	  100000	     21362 ns/op	      16 B/op	       2 allocs/op
BenchmarkSockSingleFunc-8     	  100000	     13738 ns/op	       0 B/op	       0 allocs/op
BenchmarkNetSingleFunc-8      	  100000	     21082 ns/op	       8 B/op	       1 allocs/op
BenchmarkNetSingle-8          	  100000	     21185 ns/op	       0 B/op	       0 allocs/op
```
The next question to answer is "Which connection pool implementation is most efficient?"
We put channels v sync.Pool, vs Atomic Pool (keep track of available connections in an atomic.Int), vs Semaphore Pool (using a semaphore) and lastly Dropbox's net2.Pool package.
```
BenchmarkNetChanPool-8        	  100000	     21673 ns/op	       0 B/op	       0 allocs/op
BenchmarkNetSyncPool-8        	  100000	     22677 ns/op	      32 B/op	       1 allocs/op
BenchmarkNetAtomicPool-8      	  100000	     22091 ns/op	      32 B/op	       1 allocs/op
BenchmarkNetSemPool-8         	  100000	     22075 ns/op	      32 B/op	       1 allocs/op
BenchmarkNet2Pool-8           	  100000	     23398 ns/op	     312 B/op	       5 allocs/op
```
After having picked the most efficient (using a channel for the pool) this was picked for implementation in Greddis. It was also the only one with zero allocs, so yay!
The benchmarks following is comparing Redigo, GoRedis and Greddis at different object sizes and set vs get.
```
BenchmarkGreddisGet8b-8       	  100000	     21742 ns/op	       0 B/op	       0 allocs/op
BenchmarkGoRedisGet8b-8       	   50000	     24330 ns/op	     320 B/op	      14 allocs/op
BenchmarkRedigoGet8b-8        	  100000	     22981 ns/op	     192 B/op	       9 allocs/op
BenchmarkGreddisSet8b-8       	  100000	     21551 ns/op	       0 B/op	       0 allocs/op
BenchmarkGoRedisSet8b-8       	   50000	     23553 ns/op	     226 B/op	       7 allocs/op
BenchmarkRedigoSet8b-8        	  100000	     24023 ns/op	      86 B/op	       5 allocs/op
BenchmarkGreddisSet1000b-8    	  100000	     22501 ns/op	       0 B/op	       0 allocs/op
BenchmarkGoRedisSet1000b-8    	   50000	     23873 ns/op	     226 B/op	       7 allocs/op
BenchmarkRedigoSet1000b-8     	  100000	     23167 ns/op	      86 B/op	       5 allocs/op
BenchmarkGreddisSet5000b-8    	  100000	     22692 ns/op	       1 B/op	       0 allocs/op
BenchmarkGoRedisSet5000b-8    	   50000	     35115 ns/op	     226 B/op	       7 allocs/op
BenchmarkRedigoSet5000b-8     	   50000	     33089 ns/op	      86 B/op	       5 allocs/op
BenchmarkGreddisGet5000b-8    	  100000	     23083 ns/op	       1 B/op	       0 allocs/op
BenchmarkGoRedisGet5000b-8    	   50000	     31517 ns/op	   16475 B/op	      15 allocs/op
BenchmarkRedigoGet5000b-8     	   50000	     27192 ns/op	    5563 B/op	       9 allocs/op
BenchmarkGreddisGet50000b-8   	   30000	     41397 ns/op	      14 B/op	       0 allocs/op
BenchmarkGoRedisGet50000b-8   	   20000	     66963 ns/op	  177929 B/op	      15 allocs/op
BenchmarkRedigoGet50000b-8    	   30000	     50056 ns/op	   57574 B/op	       9 allocs/op
```
