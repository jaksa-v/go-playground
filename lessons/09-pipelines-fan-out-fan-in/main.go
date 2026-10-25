// Lesson 09: Pipelines, fan-out, fan-in
//
// PIPELINE: a chain of stages connected by channels. Each stage is a goroutine
// that reads from an inbound channel, does work, writes to an outbound one, and
// closes its outbound channel when its input is exhausted. Closing cascades
// down the chain, so the whole pipeline shuts down cleanly.
//
//	gen -> square -> print
//
// FAN-OUT: several goroutines read from the SAME channel to share slow work.
// FAN-IN:  merge several channels into one. Needs a WaitGroup so the merged
//
//	channel is closed exactly once, after ALL inputs are done.
//
//	         ┌-> square ─┐
//	gen ─────┼-> square ─┼──> merge -> print
//	         └-> square ─┘
//
// Note: fan-out loses ordering. If order matters, carry an index with each item.
//
// Run: go run ./lessons/09-pipelines-fan-out-fan-in
package main

import (
	"fmt"
	"sync"
	"time"
)

// gen converts a list into a channel. Stage 1.
func gen(nums ...int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for _, n := range nums {
			out <- n
		}
	}()
	return out
}

// square is a middle stage. Pretends to be slow.
func square(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			time.Sleep(30 * time.Millisecond)
			out <- n * n
		}
	}()
	return out
}

// merge is fan-in: one goroutine per input copies to out; a final goroutine
// closes out once every copier is finished.
func merge(ins ...<-chan int) <-chan int {
	out := make(chan int)
	var wg sync.WaitGroup
	for _, in := range ins {
		wg.Go(func() {
			for v := range in {
				out <- v
			}
		})
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func main() {
	nums := []int{1, 2, 3, 4, 5, 6}

	fmt.Println("== 1. linear pipeline ==")
	start := time.Now()
	for v := range square(gen(nums...)) {
		fmt.Print(v, " ")
	}
	fmt.Printf("\n  took %v\n", time.Since(start).Round(10*time.Millisecond))

	fmt.Println("\n== 2. fan-out to 3 squarers, fan-in with merge ==")
	start = time.Now()
	src := gen(nums...)
	for v := range merge(square(src), square(src), square(src)) {
		fmt.Print(v, " ")
	}
	fmt.Printf("\n  took %v (about 3x faster, order may differ)\n", time.Since(start).Round(10*time.Millisecond))

	fmt.Println("\n== 3. stages compose: square twice ==")
	for v := range square(square(gen(1, 2, 3))) {
		fmt.Print(v, " ")
	}
	fmt.Println()
}

// Try it:
//   1. Add a `filter(in <-chan int, pred func(int) bool) <-chan int` stage.
//   2. Stop reading after the first value in part 1. Upstream goroutines stay
//      blocked forever (a leak). Lesson 12 fixes this with context cancellation.
