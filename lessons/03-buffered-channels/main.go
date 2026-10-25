// Lesson 03: Buffered channels
//
//	ch := make(chan int, 3) // buffer of 3
//
// A buffered channel is a queue with fixed capacity:
//   - send blocks only when the buffer is FULL
//   - receive blocks only when the buffer is EMPTY
//
// So sender and receiver no longer have to meet. That decouples them, but you
// lose the guarantee that "the receiver got it" when send returns.
//
// len(ch) = items currently buffered, cap(ch) = capacity. len is a snapshot
// that is stale immediately in concurrent code; use it for logging, never logic.
//
// When to buffer:
//   - you know exactly how many values will be sent (e.g. one per goroutine),
//     so senders never block even if nobody reads (prevents leaks)
//   - smoothing bursts between a fast producer and a slower consumer
//   - as a semaphore (lesson 11)
//
// Buffering does NOT fix a design that deadlocks; it only delays it.
//
// Run: go run ./lessons/03-buffered-channels
package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("== 1. sends don't block until full ==")
	ch := make(chan string, 2)
	ch <- "a" // no receiver needed
	ch <- "b"
	fmt.Printf("len=%d cap=%d\n", len(ch), cap(ch))
	// ch <- "c" // would deadlock here: buffer full and nobody receiving
	fmt.Println(<-ch, <-ch) // FIFO order

	fmt.Println("\n== 2. producer runs ahead of a slow consumer ==")
	jobs := make(chan int, 3)
	go func() {
		for i := range 6 {
			jobs <- i
			fmt.Printf("  produced %d (buffered: %d)\n", i, len(jobs))
		}
		close(jobs)
	}()
	time.Sleep(50 * time.Millisecond) // let the producer fill the buffer
	for j := range jobs {
		fmt.Println("  consumed", j)
		time.Sleep(20 * time.Millisecond)
	}

	fmt.Println("\n== 3. collect N results without blocking senders ==")
	// Buffer = number of goroutines. Each can send and exit even if we stop
	// reading early (e.g. on first error), so nothing leaks.
	urls := []string{"a.com", "b.com", "c.com"}
	results := make(chan string, len(urls))
	for _, u := range urls {
		go func() { results <- "fetched " + u }()
	}
	for range urls {
		fmt.Println(" ", <-results)
	}
}

// Try it:
//   1. Change the buffer in part 2 to 0, then to 10. Watch how output interleaving changes.
//   2. Uncomment `ch <- "c"` in part 1. Read the deadlock message.
