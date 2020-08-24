# Greddis

[![Build Status](https://github.com/mikn/greddis/workflows/build/badge.svg)](https://github.com/mikn/greddis/actions)
[![codecov](https://codecov.io/gh/mikn/greddis/branch/master/graph/badge.svg)](https://codecov.io/gh/mikn/greddis)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/mikn/greddis.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/mikn/greddis/alerts/)
[![FOSSA Status](https://app.fossa.com/api/projects/custom%2B13944%2Fgreddis.svg?type=shield)](https://app.fossa.com/projects/custom%2B13944%2Fgreddis?ref=badge_shield)
[![Go Report Card](https://goreportcard.com/badge/github.com/mikn/greddis)](https://goreportcard.com/report/github.com/mikn/greddis)
[![Go doc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/mikn/greddis)

**Note**: Currently Greddis only implements Del/Set/Get and is more of a proof of concept than a fully implemented client.

Greddis focus is high performance and letting the user of the library control its allocations. It is built using learnings from `database/sql/driver` implementations in the standard package. Many implementations of these interfaces provide consistent and high performance through good use of buffer pools, as well as the connection pool implementation in the standard library itself.

Furthermore, it is compatible with any implementation of Valuer/Scanner from `database/sql` as long as they have a `[]byte` implementation (as all data returned from Redis is `[]byte`).

## Roadmap
 - [x] Pub/sub commands
 - [ ] Sentinel support
 - [ ] Set commands
 - [ ] List commands

## Helping out?
To run the unit tests
```
$ go get .
$ go generate
$ go test
```
And to run the integration tests
```
$ go test -tags=integration
```
To generate a coverage report
```
$ go test -coverprofile=coverage.out
$ go tool cover -html=coverage.out
```

To run the benchmarks
```
$ cd benchmarks/
$ go test -bench=.
```


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
| GoRedis | Yes |

According to the Redis Serialization Protocol ([RESP Specification](https://redis.io/topics/protocol)), client libraries should use the RESP protocol to make requests as well as parse it for responses. The other option is to use their "human readable" Telnet protocol, which **Redigo** implements. The problem is that this does not allow the Redis server to up-front allocate the entire memory section required to store the request before parsing it, and thus it needs to iteratively parse the return in chunks until it reaches the end.

### Pools request and response buffers to amortize allocation cost?

| Client  |     |
| ------- | --- |
| Greddis | Yes |
| Redigo  | No  |
| GoRedis | No  |

Neither **Redigo** nor **GoRedis** pools its buffers for reuse and allocates them on the stack per request. This becomes rather heavy on performance, especially as response sizes grow. You can see in the Get benchmarks for the three clients that **GoRedis** allocates the result no less than three times on the stack and **Redigo** allocates once. (Reference, *Get10000b* benchmark)

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
BenchmarkNetSingleBufIO-8   	   76858	     14251 ns/op	      16 B/op	       2 allocs/op
BenchmarkSockSingleFunc-8   	  126729	      8299 ns/op	       0 B/op	       0 allocs/op
BenchmarkNetSingleFunc-8    	   80509	     14925 ns/op	       8 B/op	       1 allocs/op
BenchmarkNetSingle-8        	   82456	     14629 ns/op	       0 B/op	       0 allocs/op
```
The next question to answer is "Which connection pool implementation is most efficient?"
We put channels v sync.Pool, vs Atomic Pool (keep track of available connections in an atomic.Int), vs Semaphore Pool (using a semaphore) and lastly Dropbox's net2.Pool package.
```
BenchmarkNetChanPool-8      	   72476	     15093 ns/op	       0 B/op	       0 allocs/op
BenchmarkNetSyncPool-8      	   74612	     15654 ns/op	      32 B/op	       1 allocs/op
BenchmarkNetAtomicPool-8    	   81070	     15285 ns/op	      32 B/op	       1 allocs/op
BenchmarkNetSemPool-8       	   79828	     15712 ns/op	      32 B/op	       1 allocs/op
BenchmarkNet2Pool-8         	   77632	     16344 ns/op	     312 B/op	       5 allocs/op

```
After having picked the most efficient (using a channel for the pool) this was picked for implementation in Greddis. It was also the only one with zero allocs, so yay!
The benchmarks following is comparing Redigo, GoRedis and Greddis at different object sizes doing pubsub, set and get.
```
BenchmarkDrivers/GoRedisPubSub1000b-8         	   50485	     22692 ns/op	    1520 B/op	      20 allocs/op
BenchmarkDrivers/GoRedisPubSub10000b-8        	   38638	     31074 ns/op	   10736 B/op	      20 allocs/op
BenchmarkDrivers/GoRedisPubSub100000b-8       	   17682	     67689 ns/op	  106993 B/op	      20 allocs/op
BenchmarkDrivers/GoRedisPubSub10000000b-8     	     150	   7995486 ns/op	10003132 B/op	      21 allocs/op
BenchmarkDrivers/RedigoPubSub1000b-8          	   71019	     16286 ns/op	    1480 B/op	      18 allocs/op
BenchmarkDrivers/RedigoPubSub10000b-8         	   43623	     25570 ns/op	   10696 B/op	      18 allocs/op
BenchmarkDrivers/RedigoPubSub100000b-8        	    9266	    109194 ns/op	  106966 B/op	      18 allocs/op
BenchmarkDrivers/RedigoPubSub10000000b-8      	     100	  12289723 ns/op	10103156 B/op	      19 allocs/op
BenchmarkDrivers/GreddisPubSub1000b-8         	   54388	     19923 ns/op	       2 B/op	       0 allocs/op
BenchmarkDrivers/GreddisPubSub10000b-8        	   41598	     27145 ns/op	       4 B/op	       0 allocs/op
BenchmarkDrivers/GreddisPubSub100000b-8       	   20304	     58576 ns/op	      25 B/op	       0 allocs/op
BenchmarkDrivers/GreddisPubSub10000000b-8     	     166	   7100689 ns/op	  262974 B/op	       2 allocs/op
BenchmarkDrivers/GoRedisGet1000b-8            	   72638	     15873 ns/op	    3348 B/op	      15 allocs/op
BenchmarkDrivers/GoRedisGet10000b-8           	   52407	     22818 ns/op	   31222 B/op	      15 allocs/op
BenchmarkDrivers/GoRedisGet100000b-8          	   12256	    126058 ns/op	  426675 B/op	      18 allocs/op
BenchmarkDrivers/GoRedisGet10000000b-8        	     148	   7967071 ns/op	40011394 B/op	      21 allocs/op
BenchmarkDrivers/GoRedisSet1000b-8            	   64404	     17622 ns/op	     226 B/op	       7 allocs/op
BenchmarkDrivers/GoRedisSet10000b-8           	   50204	     24055 ns/op	     226 B/op	       7 allocs/op
BenchmarkDrivers/GoRedisSet100000b-8          	   26132	     47400 ns/op	     226 B/op	       7 allocs/op
BenchmarkDrivers/GoRedisSet10000000b-8        	     332	   4064425 ns/op	     282 B/op	       7 allocs/op
BenchmarkDrivers/RedigoGet1000b-8             	   69430	     18184 ns/op	    1220 B/op	       9 allocs/op
BenchmarkDrivers/RedigoGet10000b-8            	   49509	     24123 ns/op	   10443 B/op	       9 allocs/op
BenchmarkDrivers/RedigoGet100000b-8           	   21214	     59771 ns/op	  106758 B/op	       9 allocs/op
BenchmarkDrivers/RedigoGet10000000b-8         	     288	   4908264 ns/op	10003177 B/op	      11 allocs/op
BenchmarkDrivers/RedigoSet1000b-8             	   70467	     16785 ns/op	      99 B/op	       5 allocs/op
BenchmarkDrivers/RedigoSet10000b-8            	   51565	     22755 ns/op	      99 B/op	       5 allocs/op
BenchmarkDrivers/RedigoSet100000b-8           	   12538	     98983 ns/op	     100 B/op	       5 allocs/op
BenchmarkDrivers/RedigoSet10000000b-8         	     138	   8812137 ns/op	     216 B/op	       5 allocs/op
BenchmarkDrivers/GreddisGet1000b-8            	   68478	     17082 ns/op	       1 B/op	       0 allocs/op
BenchmarkDrivers/GreddisGet10000b-8           	   65054	     19505 ns/op	       1 B/op	       0 allocs/op
BenchmarkDrivers/GreddisGet100000b-8          	   30398	     39965 ns/op	      14 B/op	       0 allocs/op
BenchmarkDrivers/GreddisGet10000000b-8        	     276	   4067938 ns/op	  157934 B/op	       0 allocs/op
BenchmarkDrivers/GreddisSet1000b-8            	   69969	     16474 ns/op	       1 B/op	       0 allocs/op
BenchmarkDrivers/GreddisSet10000b-8           	   47844	     23079 ns/op	       1 B/op	       0 allocs/op
BenchmarkDrivers/GreddisSet100000b-8          	   25050	     47438 ns/op	       3 B/op	       0 allocs/op
BenchmarkDrivers/GreddisSet10000000b-8        	     340	   4707750 ns/op	     129 B/op	       0 allocs/op
```
