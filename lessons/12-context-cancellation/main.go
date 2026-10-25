// Lesson 12: context.Context, cancellation and deadlines
//
// Goroutines can't be killed from outside. They must cooperate: check a
// signal and return. context.Context is the standard signal, passed as the
// first argument down the call chain.
//
//	ctx, cancel := context.WithCancel(parent)     // cancel() manually
//	ctx, cancel := context.WithTimeout(parent, d)  // cancels after d
//	defer cancel()                                 // ALWAYS, frees resources
//
//	<-ctx.Done()   // a channel closed on cancellation (close = broadcast, lesson 04)
//	ctx.Err()      // context.Canceled or context.DeadlineExceeded
//
// Cancelling a parent cancels all its children, so one cancel() at the top
// stops a whole tree of goroutines.
//
// The core idiom: every blocking send/receive in a goroutine goes in a select
// with <-ctx.Done(), so the goroutine can always exit.
//
// Run: go run ./lessons/12-context-cancellation
package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// gen produces numbers forever until ctx is cancelled. Without the ctx case,
// a consumer that stops reading would leave this goroutine blocked forever.
func gen(ctx context.Context) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		defer fmt.Println("  gen: exiting cleanly")
		for i := 0; ; i++ {
			select {
			case out <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// slowQuery simulates a DB call that respects cancellation.
func slowQuery(ctx context.Context, d time.Duration) (string, error) {
	select {
	case <-time.After(d):
		return "rows", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func main() {
	fmt.Println("== 1. stop an infinite generator ==")
	ctx, cancel := context.WithCancel(context.Background())
	for n := range gen(ctx) {
		fmt.Println("  got", n)
		if n == 3 {
			break
		}
	}
	cancel()
	time.Sleep(10 * time.Millisecond) // let gen print its exit message

	fmt.Println("\n== 2. timeout ==")
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := slowQuery(ctx, 200*time.Millisecond); errors.Is(err, context.DeadlineExceeded) {
		fmt.Println("  query gave up:", err)
	}
	res, err := slowQuery(context.Background(), 10*time.Millisecond)
	fmt.Println("  fast query:", res, err)

	fmt.Println("\n== 3. parent cancels a tree of workers ==")
	parent, cancelAll := context.WithCancel(context.Background())
	done := make(chan string)
	for _, name := range []string{"cache", "db", "api"} {
		child, cancelChild := context.WithTimeout(parent, time.Second) // a child context
		go func() {
			defer cancelChild()
			<-child.Done()
			done <- fmt.Sprintf("  %s stopped: %v", name, child.Err())
		}()
	}
	time.Sleep(20 * time.Millisecond)
	cancelAll() // one call stops all three
	for range 3 {
		fmt.Println(<-done)
	}

	fmt.Println("\n== 4. context.Cause: say WHY it was cancelled ==")
	ctx, cancelCause := context.WithCancelCause(context.Background())
	cancelCause(errors.New("user hit ctrl-c"))
	fmt.Println("  err:", ctx.Err(), "| cause:", context.Cause(ctx))
}

// Try it:
//   1. Remove the `case <-ctx.Done()` in gen. Does "gen: exiting cleanly" print?
//   2. Go back to lesson 09 and make every stage take a ctx.
//   3. Conventions: ctx is the first parameter, named ctx, never stored in a struct.
