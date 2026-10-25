// Lesson 11: Semaphores and rate limiting
//
// SEMAPHORE: limit how many goroutines do something at once, without a fixed
// worker pool. A buffered channel of capacity N is a semaphore:
//
//	sem := make(chan struct{}, N)
//	sem <- struct{}{}   // acquire (blocks when N are held)
//	<-sem               // release
//
// Pool vs semaphore: a pool has N long-lived goroutines; a semaphore lets you
// start one goroutine per task but only N run the critical part at once.
// Semaphores are often simpler when tasks are heterogeneous.
//
// RATE LIMITING: limit how OFTEN, not how many. time.Ticker gives you one
// token per interval. A buffered channel filled by a ticker allows bursts.
// (In production, golang.org/x/time/rate is the standard token bucket.)
//
// Run: go run ./lessons/11-semaphores-and-rate-limiting
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	fmt.Println("== 1. semaphore: 10 tasks, at most 3 at a time ==")
	sem := make(chan struct{}, 3)
	var running, peak atomic.Int32
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			now := running.Add(1)
			for {
				p := peak.Load()
				if now <= p || peak.CompareAndSwap(p, now) {
					break
				}
			}
			fmt.Printf("  task %d running (concurrent: %d)\n", i, now)
			time.Sleep(30 * time.Millisecond)
			running.Add(-1)
		})
	}
	wg.Wait()
	fmt.Println("  peak concurrency:", peak.Load())

	fmt.Println("\n== 2. steady rate: one request every 50ms ==")
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	start := time.Now()
	for i := range 5 {
		<-tick.C
		fmt.Printf("  request %d at %v\n", i, time.Since(start).Round(10*time.Millisecond))
	}

	fmt.Println("\n== 3. bursty limiter: 3 instant, then one every 50ms ==")
	tokens := make(chan struct{}, 3)
	for range 3 {
		tokens <- struct{}{} // pre-fill the burst
	}
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				select {
				case tokens <- struct{}{}: // refill
				default: // bucket full, drop the token
				}
			case <-stop:
				return
			}
		}
	}()
	start = time.Now()
	for i := range 6 {
		<-tokens
		fmt.Printf("  request %d at %v\n", i, time.Since(start).Round(10*time.Millisecond))
	}
	close(stop)
}

// Try it:
//   1. Change the semaphore capacity to 1. What did you just build? (a mutex)
//   2. Combine both: at most 2 concurrent AND at most 10 per second.
