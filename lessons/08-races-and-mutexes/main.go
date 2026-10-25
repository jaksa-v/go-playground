// Lesson 08: Data races, Mutex, atomic, and when to pick channels
//
// A DATA RACE happens when two goroutines access the same memory at the same
// time and at least one writes. The result is undefined: lost updates, torn
// reads, crashes. `counter++` is read-modify-write, three steps, not one.
//
// The race detector finds them at runtime:
//
//	go run -race ./lessons/08-races-and-mutexes
//
// Three ways to fix shared state:
//   - sync.Mutex     lock around the critical section. Simple, fast, the default for "protect this field".
//   - sync/atomic    for single numbers/pointers. Fastest, but only for one value at a time.
//   - a channel      give ONE goroutine ownership of the state; others send it requests.
//
// Rule of thumb: channels for passing ownership of data and coordinating work;
// mutexes for guarding a small piece of shared state. Don't force everything
// through channels; a mutex is often simpler.
//
// Run: go run ./lessons/08-races-and-mutexes   (then again with -race)
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const n = 1000

func main() {
	fmt.Println("== 1. racy counter (expect less than 1000; -race reports it) ==")
	racy := 0
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() { racy++ })
	}
	wg.Wait()
	fmt.Println("  racy:", racy)

	fmt.Println("\n== 2. sync.Mutex ==")
	var mu sync.Mutex
	locked := 0
	for range n {
		wg.Go(func() {
			mu.Lock()
			defer mu.Unlock()
			locked++
		})
	}
	wg.Wait()
	fmt.Println("  mutex:", locked)

	fmt.Println("\n== 3. sync/atomic ==")
	var at atomic.Int64
	for range n {
		wg.Go(func() { at.Add(1) })
	}
	wg.Wait()
	fmt.Println("  atomic:", at.Load())

	fmt.Println("\n== 4. channel: one goroutine owns the state ==")
	incr := make(chan struct{})
	total := make(chan int)
	go func() {
		count := 0 // only this goroutine ever touches count
		for range incr {
			count++
		}
		total <- count
	}()
	for range n {
		wg.Go(func() { incr <- struct{}{} })
	}
	wg.Wait()
	close(incr)
	fmt.Println("  channel:", <-total)

	fmt.Println("\n== 5. a struct guarding its own state (the common real-world shape) ==")
	c := SafeCache{m: map[string]int{}}
	for i := range 100 {
		wg.Go(func() { c.Set(fmt.Sprint(i%10), i) })
	}
	wg.Wait()
	fmt.Println("  keys:", c.Len())

	fmt.Println("\n== 6. sync.Once: run init exactly once, even when called concurrently ==")
	var once sync.Once
	for range 5 {
		wg.Go(func() { once.Do(func() { fmt.Println("  initialized (prints once)") }) })
	}
	wg.Wait()
}

// SafeCache keeps the mutex next to the data it protects and never exposes
// the map, so callers can't forget to lock. RWMutex allows many concurrent
// readers OR one writer; worth it only for read-heavy workloads.
type SafeCache struct {
	mu sync.RWMutex
	m  map[string]int
}

func (c *SafeCache) Set(k string, v int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[k] = v
}

func (c *SafeCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

// Try it:
//   1. Run with -race and read the report: it shows both goroutines' stacks.
//   2. Remove the mutex from SafeCache.Set. Run with -race. Maps panic on
//      concurrent writes even without -race ("concurrent map writes").
//   3. Never copy a struct containing a Mutex (pass *SafeCache). `go vet` catches it.
