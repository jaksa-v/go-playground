// Lesson 06: select
//
// select waits on several channel operations at once and runs whichever is
// ready first. If several are ready, it picks one at RANDOM (no priority, which
// prevents starvation). It is the switch statement of concurrency.
//
//	select {
//	case v := <-a:        // receive
//	case b <- x:          // send
//	case <-time.After(d): // timeout
//	default:              // runs immediately if nothing else is ready (non-blocking)
//	}
//
// An empty `select {}` blocks forever.
//
// Run: go run ./lessons/06-select
package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("== 1. whichever is ready first ==")
	fast, slow := make(chan string), make(chan string)
	go func() { time.Sleep(20 * time.Millisecond); fast <- "fast" }()
	go func() { time.Sleep(60 * time.Millisecond); slow <- "slow" }()
	for range 2 {
		select {
		case m := <-fast:
			fmt.Println("  got", m)
		case m := <-slow:
			fmt.Println("  got", m)
		}
	}

	fmt.Println("\n== 2. timeout ==")
	never := make(chan int)
	select {
	case v := <-never:
		fmt.Println("  got", v)
	case <-time.After(50 * time.Millisecond):
		fmt.Println("  timed out after 50ms")
	}

	fmt.Println("\n== 3. non-blocking send/receive with default ==")
	queue := make(chan int, 1)
	for i := range 3 {
		select {
		case queue <- i:
			fmt.Println("  queued", i)
		default:
			fmt.Println("  queue full, dropped", i) // load shedding
		}
	}
	select {
	case v := <-queue:
		fmt.Println("  polled", v)
	default:
		fmt.Println("  nothing to poll")
	}

	fmt.Println("\n== 4. random choice when several are ready ==")
	a, b := make(chan int, 100), make(chan int, 100)
	for range 100 {
		a <- 1
		b <- 1
	}
	counts := map[string]int{}
	for range 100 {
		select {
		case <-a:
			counts["a"]++
		case <-b:
			counts["b"]++
		}
	}
	fmt.Println("  picks:", counts, "(roughly 50/50)")

	fmt.Println("\n== 5. loop with a quit channel (the classic event loop) ==")
	ticks := time.NewTicker(15 * time.Millisecond)
	defer ticks.Stop()
	quit := make(chan struct{})
	go func() { time.Sleep(70 * time.Millisecond); close(quit) }()
loop:
	for {
		select {
		case t := <-ticks.C:
			fmt.Println("  tick", t.Format("05.000"))
		case <-quit:
			fmt.Println("  quit")
			break loop // plain `break` would only exit the select!
		}
	}
}

// Try it:
//   1. In part 5 replace `break loop` with `break`. What happens?
//   2. Write a select that sends on `out` but gives up after 100ms.
//   3. Note: time.After inside a hot loop allocates a timer per iteration.
//      For a per-iteration timeout that's fine; for one overall deadline,
//      create the timer once outside the loop.
