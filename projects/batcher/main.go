// Project: batcher (flush by size OR by time)
//
// Writing rows to a database or sending metrics one by one is slow. A batcher
// collects items and flushes them when EITHER the batch is full OR a max wait
// has passed since the first item arrived, whichever comes first. On shutdown
// it flushes what's left so nothing is lost.
//
// You'll find this inside log shippers, Kafka producers, analytics SDKs, etc.
//
// Concepts combined: select over input + timer (06), nil channel to disable the
// timer case while the batch is empty (07), close to drain and shut down (04),
// a done channel to wait for the final flush (02).
//
// Run: go run ./projects/batcher
package main

import (
	"fmt"
	"time"
)

type Batcher[T any] struct {
	in   chan T
	done chan struct{}
}

// NewBatcher starts a goroutine that calls flush with batches of up to maxSize
// items, never holding an item longer than maxWait.
func NewBatcher[T any](maxSize int, maxWait time.Duration, flush func([]T)) *Batcher[T] {
	b := &Batcher[T]{in: make(chan T), done: make(chan struct{})}
	go func() {
		defer close(b.done)
		batch := make([]T, 0, maxSize)
		var timer <-chan time.Time // nil while batch is empty: that case is disabled

		send := func() {
			if len(batch) > 0 {
				flush(batch)
				batch = make([]T, 0, maxSize) // fresh slice; flush may keep the old one
			}
			timer = nil
		}

		for {
			select {
			case item, ok := <-b.in:
				if !ok { // Close was called: final flush, then exit
					send()
					return
				}
				if len(batch) == 0 {
					timer = time.After(maxWait) // clock starts at the first item
				}
				batch = append(batch, item)
				if len(batch) == maxSize {
					send()
				}
			case <-timer:
				send()
			}
		}
	}()
	return b
}

func (b *Batcher[T]) Add(item T) { b.in <- item }

// Close stops accepting items and blocks until the final flush has finished.
func (b *Batcher[T]) Close() {
	close(b.in)
	<-b.done
}

func main() {
	start := time.Now()
	b := NewBatcher(3, 100*time.Millisecond, func(batch []string) {
		fmt.Printf("  %4dms flush %v\n", time.Since(start).Milliseconds(), batch)
	})

	fmt.Println("burst of 7 events: two flushes by SIZE")
	for i := range 7 {
		b.Add(fmt.Sprintf("e%d", i))
	}
	fmt.Println("... the leftover e6 flushes by TIME after ~100ms")
	time.Sleep(150 * time.Millisecond)

	fmt.Println("trickle, one every 40ms: flushes by time or size, whichever first")
	for i := range 5 {
		b.Add(fmt.Sprintf("t%d", i))
		time.Sleep(40 * time.Millisecond)
	}

	fmt.Println("shutdown: Close flushes whatever is pending")
	b.Add("last")
	b.Close()
}

// Try it:
//   1. Make flush slow (sleep 200ms). Add() now blocks callers during a flush.
//      Is that backpressure good or bad here? How would you avoid it?
//   2. Add a Flush() method that forces an immediate flush (hint: another channel).
