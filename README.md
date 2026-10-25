# Go concurrency playground

Learn goroutines and channels by running and breaking small programs. Each lesson is one `main.go`: the header comment explains the concept, the code demonstrates it, and a "Try it" block at the bottom tells you what to change and observe.

Go through them in order. Run each one, read the code, then do the "Try it" items.

```bash
go run ./lessons/01-goroutines
```

Add `-race` to any run to turn on the race detector (lesson 08 is the one where it fires on purpose).

## Lessons

| # | Lesson | What you learn |
|---|--------|----------------|
| 01 | [goroutines](lessons/01-goroutines/main.go) | `go`, main exiting, `sync.WaitGroup`, `wg.Go`, no ordering |
| 02 | [unbuffered-channels](lessons/02-unbuffered-channels/main.go) | send/receive, rendezvous, done signals, returning results |
| 03 | [buffered-channels](lessons/03-buffered-channels/main.go) | capacity, when to buffer, collecting N results |
| 04 | [close-and-range](lessons/04-close-and-range/main.go) | `close`, `for range`, comma-ok, close as broadcast, panic rules |
| 05 | [directional-channels](lessons/05-directional-channels/main.go) | `chan<-`, `<-chan`, channel ownership |
| 06 | [select](lessons/06-select/main.go) | multiplexing, timeouts, non-blocking ops, event loops |
| 07 | [nil-channels](lessons/07-nil-channels/main.go) | disabling select cases, merging until both closed |
| 08 | [races-and-mutexes](lessons/08-races-and-mutexes/main.go) | data races, `-race`, `Mutex`, `RWMutex`, `atomic`, `Once`, channels vs mutexes |
| 09 | [pipelines-fan-out-fan-in](lessons/09-pipelines-fan-out-fan-in/main.go) | stages, fan-out, fan-in with `merge` |
| 10 | [worker-pool](lessons/10-worker-pool/main.go) | fixed workers, correct shutdown order |
| 11 | [semaphores-and-rate-limiting](lessons/11-semaphores-and-rate-limiting/main.go) | bounded concurrency, tickers, token bucket |
| 12 | [context-cancellation](lessons/12-context-cancellation/main.go) | `context`, timeouts, cancelling goroutine trees |
| 13 | [leaks-and-deadlocks](lessons/13-leaks-and-deadlocks/main.go) | finding leaks, reading deadlock dumps |
| 14 | [errors-and-errgroup](lessons/14-errors-and-errgroup/main.go) | errors from goroutines, building errgroup, panics |

## Projects

Real-world programs that combine the lessons. Read these after lesson 14.

| Project | What it is |
|---------|-----------|
| [batcher](projects/batcher/main.go) | Collect items, flush by size or time. Used in log shippers, DB writers, metrics SDKs. |
| [pubsub](projects/pubsub/main.go) | In-memory message broker with the actor pattern and slow-subscriber handling. |
| [crawler](projects/crawler/main.go) | Concurrent crawler with dedup, bounded concurrency and a global timeout. |

```bash
go run ./projects/batcher
```

## The rules worth memorizing

- Before starting a goroutine, know how it will exit.
- Only the sender closes a channel. With many senders, close after a `WaitGroup`.
- Send on closed channel, close of closed channel, close of nil channel: all panic.
- Receive on closed channel returns the zero value immediately; use `v, ok := <-ch`.
- Nil channel blocks forever; in a `select` that disables the case.
- Every blocking send/receive in a long-lived goroutine sits in a `select` with `<-ctx.Done()`.
- Mutex for guarding state, channels for passing ownership and coordinating work.
- A panic in any goroutine kills the whole program.

## Further reading

- [Go Memory Model](https://go.dev/ref/mem)
- [Pipelines and cancellation](https://go.dev/blog/pipelines)
- [Concurrency is not parallelism](https://go.dev/blog/waza-talk) (Rob Pike talk)
- [Share memory by communicating](https://go.dev/blog/codelab-share)
