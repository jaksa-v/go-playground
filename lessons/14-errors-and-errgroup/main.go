// Lesson 14: Errors from goroutines, and the errgroup pattern
//
// A goroutine can't return a value to its caller, so errors must travel
// through a channel or shared variable. The common needs:
//   - run N tasks concurrently
//   - wait for all
//   - return the FIRST error
//   - cancel the others as soon as one fails
//
// That is exactly golang.org/x/sync/errgroup. We build a tiny version here so
// you see there's no magic, then you'd use the real one in production:
//
//	g, ctx := errgroup.WithContext(ctx)
//	g.SetLimit(4) // optional: bounded concurrency
//	g.Go(func() error { ... })
//	err := g.Wait()
//
// Run: go run ./lessons/14-errors-and-errgroup
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Group is a minimal errgroup: WaitGroup + first error + cancel on failure.
type Group struct {
	wg      sync.WaitGroup
	errOnce sync.Once
	err     error
	cancel  context.CancelFunc
}

func WithContext(ctx context.Context) (*Group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{cancel: cancel}, ctx
}

func (g *Group) Go(f func() error) {
	g.wg.Go(func() {
		if err := f(); err != nil {
			g.errOnce.Do(func() {
				g.err = err
				g.cancel() // tell the siblings to stop
			})
		}
	})
}

func (g *Group) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}

// fetch pretends to call a service. "payments" fails.
func fetch(ctx context.Context, name string, d time.Duration) error {
	select {
	case <-time.After(d):
		if name == "payments" {
			return fmt.Errorf("%s: 503 service unavailable", name)
		}
		fmt.Println("  ok:", name)
		return nil
	case <-ctx.Done():
		fmt.Println("  cancelled:", name)
		return ctx.Err()
	}
}

func main() {
	fmt.Println("== 1. collect ALL errors via a channel ==")
	errs := make(chan error, 3) // buffered: senders never block
	var wg sync.WaitGroup
	for _, name := range []string{"a", "b", "payments"} {
		wg.Go(func() { errs <- fetch(context.Background(), name, 10*time.Millisecond) })
	}
	wg.Wait()
	close(errs)
	var all []error
	for err := range errs {
		if err != nil {
			all = append(all, err)
		}
	}
	fmt.Println("  joined:", errors.Join(all...))

	fmt.Println("\n== 2. errgroup: first error cancels the rest ==")
	g, ctx := WithContext(context.Background())
	g.Go(func() error { return fetch(ctx, "users", 10*time.Millisecond) })
	g.Go(func() error { return fetch(ctx, "payments", 30*time.Millisecond) })
	g.Go(func() error { return fetch(ctx, "reports", 500*time.Millisecond) }) // gets cancelled
	start := time.Now()
	err := g.Wait()
	fmt.Printf("  first error: %v (after %v, not 500ms)\n", err, time.Since(start).Round(10*time.Millisecond))

	fmt.Println("\n== 3. panics in goroutines crash the WHOLE program ==")
	// A recover() in main can't catch a panic in another goroutine.
	// If a goroutine might panic, recover inside it and turn it into an error.
	res := make(chan error)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				res <- fmt.Errorf("recovered: %v", r)
			}
		}()
		var m map[string]int
		m["boom"] = 1 // nil map write panics
	}()
	fmt.Println(" ", <-res)
}

// Try it:
//   1. Add SetLimit(n) to Group using a semaphore channel (lesson 11).
//   2. Then replace Group with the real one: go get golang.org/x/sync/errgroup
