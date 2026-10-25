// Lesson 13: Goroutine leaks and deadlocks
//
// LEAK: a goroutine blocked forever (on a send nobody receives, a receive
// nobody sends, or a lock nobody releases). It never exits, holds its memory,
// and the runtime can't detect it while other goroutines are still alive.
// Leaks are the #1 concurrency bug in long-running Go services.
//
// DEADLOCK: every goroutine is blocked. If ALL are blocked, the runtime
// crashes with "fatal error: all goroutines are asleep - deadlock!". Partial
// deadlocks (some goroutines stuck, others fine) are silent, i.e. leaks.
//
// Before starting a goroutine, ask: "How and when will it exit?"
//
// Run: go run ./lessons/13-leaks-and-deadlocks
// Run a deadlock demo: go run ./lessons/13-leaks-and-deadlocks deadlock1   (or deadlock2, deadlock3)
package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// leakyFirst returns the first result and abandons the rest. With an
// unbuffered channel, the slower senders block forever.
func leakyFirst() string {
	ch := make(chan string)
	for _, s := range []string{"replica-a", "replica-b", "replica-c"} {
		go func() { ch <- s }() // two of these never complete
	}
	return <-ch
}

// fixedFirst: buffer = number of senders, so every send completes and every
// goroutine exits whether or not anyone reads.
func fixedFirst() string {
	servers := []string{"replica-a", "replica-b", "replica-c"}
	ch := make(chan string, len(servers))
	for _, s := range servers {
		go func() { ch <- s }()
	}
	return <-ch
}

// leakyTicker forgets to give its goroutine an exit path.
func leakyTicker() {
	go func() {
		for {
			time.Sleep(time.Millisecond) // runs forever
		}
	}()
}

// fixedTicker exits when ctx is cancelled.
func fixedTicker(ctx context.Context) {
	go func() {
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
			case <-ctx.Done():
				return
			}
		}
	}()
}

func main() {
	if len(os.Args) > 1 {
		runDeadlock(os.Args[1])
		return
	}

	base := runtime.NumGoroutine()
	report := func(label string) {
		time.Sleep(20 * time.Millisecond) // let goroutines settle
		fmt.Printf("  %-22s goroutines above baseline: %d\n", label, runtime.NumGoroutine()-base)
	}

	fmt.Println("== leaks, measured with runtime.NumGoroutine ==")
	for range 100 {
		leakyFirst()
	}
	report("after 100 leakyFirst:") // ~200 leaked

	base = runtime.NumGoroutine()
	for range 100 {
		fixedFirst()
	}
	report("after 100 fixedFirst:") // 0

	base = runtime.NumGoroutine()
	for range 10 {
		leakyTicker()
	}
	report("after 10 leakyTicker:") // 10

	base = runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	for range 10 {
		fixedTicker(ctx)
	}
	cancel()
	report("after 10 fixedTicker:") // 0

	fmt.Println("\nIn tests, go.uber.org/goleak fails a test that leaks goroutines.")
	fmt.Println("Now try: go run ./lessons/13-leaks-and-deadlocks deadlock1")
}

func runDeadlock(which string) {
	switch which {
	case "deadlock1":
		// Send on unbuffered channel with no receiver, in the only goroutine.
		ch := make(chan int)
		ch <- 1
		fmt.Println(<-ch)

	case "deadlock2":
		// Forgot to close: range waits for more values forever.
		ch := make(chan int)
		go func() {
			for i := range 3 {
				ch <- i
			}
			// close(ch) // <- the fix
		}()
		for v := range ch {
			fmt.Println(v)
		}

	case "deadlock3":
		// Lock ordering: A holds mu1 wants mu2, B holds mu2 wants mu1.
		// Fix: always acquire locks in the same order.
		var mu1, mu2 sync.Mutex
		var wg sync.WaitGroup
		wg.Go(func() {
			mu1.Lock()
			time.Sleep(10 * time.Millisecond)
			mu2.Lock()
		})
		wg.Go(func() {
			mu2.Lock()
			time.Sleep(10 * time.Millisecond)
			mu1.Lock()
		})
		wg.Wait()

	default:
		fmt.Println("unknown demo, use deadlock1, deadlock2 or deadlock3")
	}
}

// Try it:
//   1. Run each deadlock demo and read the goroutine dump. It tells you
//      exactly where each goroutine is stuck ("chan send", "chan receive", "sync.Mutex.Lock").
//   2. Fix leakyFirst using a context instead of a buffer.
