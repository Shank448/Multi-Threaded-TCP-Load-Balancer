# Multi-Threaded TCP Load Balancer

A layer-4 (TCP) load balancer written from scratch in **C++17** using **POSIX threads** and raw
**BSD sockets** — no frameworks, no third-party libraries. It accepts client connections, spreads
them across a pool of backend servers, survives backend failures, and shuts down gracefully.

```
 clients ──► accept loop ──► bounded queue ──► worker pool (pthreads)
                                                   │ 1. pick backend (round-robin / least-connections)
                                                   │ 2. connect with timeout; on failure fail over
                                                   ▼
                                       poll()-driven bidirectional relay ──► backend servers
 health-checker thread ── probes every backend, marks UP / DOWN ──┘
```

## Features

| Area | What it does |
|---|---|
| Concurrency | Fixed `pthread` worker pool; one mutex + condition variable guard a **bounded** task queue |
| Load balancing | Round-robin and least-connections; selection is mutex-protected so workers can't herd onto one backend |
| Reliability | Passive failover (retry next backend on connect failure) + active health checks with automatic re-admission |
| Back-pressure | Queue full => connection rejected immediately (load shedding) instead of unbounded memory/latency growth |
| Data path | Non-blocking relay in one `poll()` loop; per-direction buffers; TCP half-close propagated end-to-end |
| Operations | Graceful shutdown on SIGINT/SIGTERM (stop accepting, drain, then force), idle timeouts, periodic stats |

## Build & run

Requires Linux, macOS or WSL (POSIX), `g++` with C++17, `make`, and Python 3 for the tests.

```bash
make                      # builds build/lb, build/echo_backend, build/load_client
./scripts/demo.sh         # 3 backends + balancer + load test + simulated backend crash

# or by hand:
./build/echo_backend --port 9001 &
./build/echo_backend --port 9002 &
./build/lb --port 8080 --strategy lc --backend 127.0.0.1:9001 --backend 127.0.0.1:9002
./build/load_client --port 8080 --clients 50 --requests 100
printf 'hello\n' | nc 127.0.0.1 8080       # -> [backend-9001] hello
```

`./build/lb --help` lists every option (`--threads`, `--queue`, `--idle-timeout`, `--health-interval`, ...).

## Testing

```bash
make test    # 9 end-to-end tests over real sockets and real processes
make tsan    # same suite under ThreadSanitizer (data races / lock misuse)
make asan    # same suite under AddressSanitizer + UBSan (memory errors / UB)
```

The suite covers: exact round-robin fairness, least-connections spreading, 6 MB payload integrity,
**16 MB full-duplex streaming with SHA-256 verification** (deadlock regression test), 150 concurrent
clients, failover + automatic recovery, all-backends-down behaviour, overload shedding, and graceful
shutdown. All pass on the normal, TSan and ASan/UBSan builds.

## Benchmarks

Measured on a **single-core sandbox where the client, 3 backends and the balancer all share one core**,
so treat these as a conservative floor and re-run on your own machine (`make && ./scripts/demo.sh`).

| Scenario | Requests | Failed | Throughput | p50 | p99 |
|---|---|---|---|---|---|
| 20 clients x 200 req, 64 B | 4,000 | 0 | ~5,600 req/s | 3.3 ms | 7.7 ms |
| Same load after killing 1 of 3 backends | 4,000 | 0 | ~5,600 req/s | 3.3 ms | 7.2 ms |
| 200 concurrent clients x 50 req | 10,000 | 0 | ~4,200 req/s | 48 ms | 75 ms |
| 50 clients x 100 req, 4 KB payload | 5,000 | 0 | ~4,800 req/s | 9.4 ms | 21 ms |

Each request opens a new TCP connection, so this stresses the accept/connect path, not just copying bytes.
Distribution across 3 backends stayed within 32.9%-33.9% each.

## Design decisions (and why)

1. **Bounded queue + load shedding.** An unbounded queue turns an overload into unbounded latency and
   memory. Rejecting early keeps latency predictable for the connections that *are* admitted.
2. **Non-blocking relay in a single `poll()` loop.** My first instinct — blocking `send()` in each
   direction — can deadlock: both peers stall in `send()` while neither reads. Servicing both
   directions from one loop with non-blocking I/O and per-direction buffers removes that class of bug,
   and only reading when the buffer is empty gives natural back-pressure.
3. **Half-close propagation.** When one side sends FIN, the balancer calls `shutdown(SHUT_WR)` on the
   other side instead of closing, so request/response protocols that half-close still finish cleanly.
4. **Lock discipline (deadlock avoidance).** Each lock protects one small thing (queue; backend
   selection; log line), is never held across I/O, and no code path ever takes two locks. Per-backend
   counters are `std::atomic`, so stats and health checks never contend with the hot path.
5. **Two layers of health checking.** Passive (2 consecutive connect failures eject a backend instantly)
   plus active probes (re-admit it automatically once it recovers).
6. **Backend addresses resolved once at start-up**, so no DNS lookups on the hot path.
7. **Graceful shutdown in three phases:** stop accepting -> drain in-flight connections for `--grace`
   seconds -> force-close the rest and join all threads.

## Known limitations / possible extensions

- Thread-per-connection model: max concurrent connections = `--threads`. The next step up is an
  `epoll`-based event loop (what nginx/HAProxy do) — a great "what would you change at 100k conns?" answer.
- IPv4 only, no TLS termination, no weighted balancing, no config file, no hot reload.
- Ideas: weighted round-robin, consistent hashing for sticky sessions, Prometheus-style metrics endpoint,
  connection draining per backend, `SO_REUSEPORT` multi-acceptor.

## Layout

```
include/net.hpp            sockets, RAII Fd, connect-with-timeout, poll() relay
include/thread_pool.hpp    pthread worker pool (mutex + condvar, bounded queue)
include/backend.hpp        backend registry, RR / least-conn selection, passive health
include/health_checker.hpp active health-check thread
include/logger.hpp         thread-safe logger
src/main.cpp               accept loop, connection handler, config, graceful shutdown
tools/echo_backend.cpp     test backend (line mode shows which backend answered; raw mode streams)
tools/load_client.cpp      multi-threaded load generator (throughput, p50/p95/p99, distribution)
tests/integration_test.py  end-to-end tests
```

## Resume bullets (pick numbers you have verified on YOUR machine)

- Built a multi-threaded TCP load balancer in C++17 with POSIX threads and raw sockets: bounded
  worker pool (mutex/condvar), round-robin and least-connections scheduling, and `poll()`-based
  non-blocking bidirectional relay with back-pressure and half-close handling.
- Added fault tolerance: passive failover plus active health checks with automatic recovery, load
  shedding on overload, and graceful shutdown; sustained ~4-5k new connections/s with zero failed
  requests, including during a simulated backend crash.
- Verified correctness with a 9-test end-to-end suite (incl. 16 MB SHA-256-checked full-duplex streams)
  run clean under ThreadSanitizer and AddressSanitizer/UBSan.
