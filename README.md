# C++ TCP/UDP Epoll Server

An AI-assisted learning project in C++17 for Linux: a multi-client IPv4 TCP/UDP
uppercase echo service, level-triggered epoll, bounded thread-pool work handling,
and graceful shutdown. It reuses the thread pool from
[cpp-thread-pool-scheduler](https://github.com/Ayush1015/cpp-thread-pool-scheduler).

This is a user-space networking project, not a kernel driver, embedded firmware,
wireless stack, SIP/RTP implementation or production server. It uses Linux sockets
to exchange TCP streams and UDP datagrams; it does not implement TCP/IP itself.

## Build and run (Linux / WSL2)

Requires GCC/Clang with C++17, CMake 3.16+, a build tool and Python 3 for the CLI
test. On Ubuntu: `sudo apt install build-essential cmake python3`.
Catch2 v2.13.10 is fetched once at configure with a pinned SHA256; internet access
is needed for a fresh clone. The source zip includes the same header for offline builds.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
./build/network_server 9000
```

By default it listens only on `127.0.0.1`. Port `0` chooses an ephemeral port,
printed as `READY 127.0.0.1:<port> TCP/UDP`. Both protocols use the same port.
An optional second argument selects an IPv4 bind address. Do not expose this demo
to the public internet: there is no authentication, TLS or per-peer rate limiting.
SIGINT (Ctrl+C) or SIGTERM stops it; the CLI prints `STOPPED` after joining workers.

With Python, in a second terminal:

```python
import socket
with socket.create_connection(("127.0.0.1", 9000), timeout=3) as tcp:
    tcp.sendall(b"hello\nworld\n")
    print(tcp.makefile("rb").readline()) # b'HELLO\n'
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
    udp.settimeout(3)
    udp.sendto(b"hello", ("127.0.0.1", 9000))
    print(udp.recvfrom(4096)[0]) # b'HELLO'
```

## Protocol and limits

- TCP: newline-delimited frames, at most 4096 bytes before the newline. Responses
  preserve frame order per connection. Partial reads, coalesced frames, partial
  writes and peer write-half-close are handled. An incomplete trailing frame at
  EOF is discarded. An oversized frame or full worker queue closes that client.
- UDP: each datagram is a message, including empty and binary messages. Maximum
  4096 bytes. Oversized datagrams or queue overload are dropped. Responses may be
  lost or reordered, as expected with UDP. Nonblocking send failures are dropped;
  there is no retransmission. TCP socket send errors also close the client.
- ASCII `a`..`z` becomes uppercase. All other bytes are unchanged, including NUL.
  There is no Unicode case conversion.
- Defaults: 4 workers, 256 queued work items, at most 256 admitted TCP clients.
  The library `net::Config` lets callers change these. Each TCP client has at most
  one worker job in flight and an input buffer capped at two 4096-byte reads.
  Read interest is paused while its job or response is outstanding. Writes use
  `MSG_NOSIGNAL`; disconnected clients cannot terminate the process with SIGPIPE.
- Shutdown stops accepting/reading, drains complete frames already in application
  buffers and queued work, and allows up to 2 seconds for client response draining.
  Unread kernel-buffer bytes and incomplete frames are not promised a response.
  After the deadline, remaining clients close and accepted workers are joined.
- There are no idle/read deadlines, priority scheduling, persistence or fairness
  guarantees. An idle client occupies a slot until it disconnects or shutdown.
  UDP floods and incomplete TCP frames can exhaust capacity. Limits contain
  ordinary memory/work growth; they are not a security audit or DoS protection.

## Design

Only the epoll thread owns client sockets and their buffers. Workers transform
messages and push results to a mutex-protected completion queue, then signal an
`eventfd`. The epoll thread sends responses and updates readiness interest.
Monotonically increasing connection IDs prevent a result for a disconnected
socket from being sent to an unrelated connection reusing its file descriptor.

The scheduler uses a mutex, condition variables and a bounded FIFO work queue.
Tasks run outside the queue lock. `shutdown()` rejects new jobs, drains accepted
jobs and joins workers. Its original safety contract still applies: do not destroy
it from a worker or block every worker waiting for child jobs in the same pool.

`net::Server::run()` is single-use with one caller. `request_stop()` is thread-safe
and wakes epoll. Keep the server alive until the run thread has joined. The CLI
blocks SIGINT/SIGTERM before creating workers and uses `sigwait`, rather than
calling mutex-using code from an asynchronous signal handler.

## Tests and recorded local results

```sh
./build/network_tests
./build/network_tests '[network]'
for i in $(seq 1 30); do ./build/network_tests '[concurrency]' || exit 1; done
cmake -S . -B build-tsan -DENABLE_TSAN=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build build-tsan --parallel 2
./build-tsan/network_tests
```

Measured on Linux x86-64, GCC 11.4, Debug build, October 1, 2026:

- 30 Catch2 cases, 2,078 assertions passed (14 inherited pool cases, 16 new
  network cases). Both CTest entries passed, including CLI integration.
- 30 concurrency repetitions passed: 12 cases / 2,024 assertions each. The mixed
  client case uses 24 simultaneous TCP/UDP clients with 40 request/response pairs
  each: 960 messages per repetition. These are correctness stress checks, not
  throughput or latency benchmarks.
- 10 full-suite GCC ThreadSanitizer runs passed, 2,078 assertions each, with no
  findings in executed paths. This does not prove all paths race-free. TSAN may
  fail to initialize on unsupported hosts; don't describe an initialization
  failure as a clean run.
- The Python CLI test checks five invalid invocations, plus TCP exchanges and
  clean shutdown with both SIGINT and SIGTERM.

Tests also cover fragmented/pipelined TCP, ordering, half-close, maximum frame
size, oversize rejection, slow-reader traffic, binary/empty/maximum UDP,
client admission, disconnect churn, occupied ports, tiny-queue overload, concurrent stop and idempotent stop.
The CI workflow builds/tests on Ubuntu with GCC and Clang, plus a separate TSAN
job. Verify the actual Actions result before claiming a hosted run passed.

## Attribution and resume use

This starter was built with AI assistance. Receiving it is not independent
implementation experience. Read and run it, explain its ownership and shutdown
rules, and make and test a meaningful personal change before claiming your own
contribution. Keep that contribution separate from the AI-assisted starting code.
No interview-prep documents or walkthroughs are included.

## License

Code and README: MIT, matching the scheduler (see `LICENSE`). Catch2 retains its
Boost Software License (`third_party/Catch2-LICENSE.txt` and downloaded header).
