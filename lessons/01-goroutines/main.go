// Lesson 01: Goroutines
//
// A goroutine is a function running concurrently with the rest of the program.
// You start one with the `go` keyword. They are cheap (a few KB of stack that
// grows on demand), so starting thousands is normal.
//
// Key facts:
//   - `go f()` returns immediately. The caller does not wait for f.
//   - When main returns, the program exits. Running goroutines are killed.
//   - The scheduler decides execution order. Never rely on it.
//
// Run: go run ./lessons/01-goroutines
package main

import (
	"fmt"
	"sync"
	"time"
)

func say(who string, times int) {
	for i := range times {
		fmt.Printf("%s %d\n", who, i)
		time.Sleep(10 * time.Millisecond)
	}
}

func main() {
	fmt.Println("== 1. main does not wait ==")
	go say("background", 3)
	// Without this sleep, "background" most likely never prints: main would
	// return first and kill it. Sleeping to synchronize is a hack; see part 2.
	time.Sleep(50 * time.Millisecond)

	fmt.Println("\n== 2. waiting properly with sync.WaitGroup ==")
	// A WaitGroup is a counter: Add(n) increments, Done() decrements,
	// Wait() blocks until it reaches zero.
	var wg sync.WaitGroup
	for _, name := range []string{"alice", "bob", "carol"} {
		wg.Add(1) // Add BEFORE starting the goroutine, never inside it.
		go func() {
			defer wg.Done() // defer so Done runs even if the function panics/returns early.
			say(name, 2)
		}()
	}
	wg.Wait()

	fmt.Println("\n== 3. wg.Go (Go 1.25+): Add + go + Done in one call ==")
	var wg2 sync.WaitGroup
	for i := range 3 {
		wg2.Go(func() { fmt.Println("worker", i, "done") })
	}
	wg2.Wait()
	// Note: since Go 1.22 each loop iteration gets its own `i`, so capturing it
	// in a closure is safe. Before 1.22 all goroutines could see the last value.

	fmt.Println("\n== 4. order is not guaranteed ==")
	var wg3 sync.WaitGroup
	for i := range 5 {
		wg3.Go(func() { fmt.Print(i, " ") })
	}
	wg3.Wait()
	fmt.Println("<- run it a few times, the order changes")
}

// Try it:
//   1. Remove the time.Sleep in part 1. What happens?
//   2. In part 2, move wg.Add(1) inside the goroutine. Why can Wait() return too early?
//   3. Start 100_000 goroutines that each sleep 1s. How long does it take? (cheap!)
