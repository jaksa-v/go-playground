// Lesson 04: Closing channels, range, and the comma-ok idiom
//
// close(ch) says "no more values will be sent". It is a broadcast: EVERY
// receiver notices, which makes it the standard way to signal many goroutines.
//
// Rules (memorize these):
//
//	operation      | nil channel  | open channel        | closed channel
//	---------------+--------------+---------------------+------------------------------
//	send  ch <- v  | blocks       | blocks / succeeds   | PANIC
//	recv  <-ch     | blocks       | blocks / succeeds   | buffered values, then zero value immediately
//	close(ch)      | PANIC        | succeeds            | PANIC
//
// Consequences:
//   - Only the SENDER closes. A receiver closing can make a sender panic.
//   - With multiple senders, nobody closes until all are done (use a WaitGroup,
//     then close from one place; see lesson 09 fan-in).
//   - You don't have to close a channel. Close only when receivers need to know
//     that values are finished (e.g. to end a `for range`). GC handles the rest.
//
// Run: go run ./lessons/04-close-and-range
package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	fmt.Println("== 1. range until closed ==")
	nums := make(chan int)
	go func() {
		defer close(nums) // the sender closes, deferred so it always happens
		for i := 1; i <= 3; i++ {
			nums <- i
		}
	}()
	for n := range nums { // exits the loop when nums is closed and drained
		fmt.Println(" ", n)
	}

	fmt.Println("\n== 2. comma-ok: telling 'zero value' from 'closed' ==")
	ch := make(chan int, 2)
	ch <- 0
	close(ch)
	v, ok := <-ch
	fmt.Printf("  v=%d ok=%v  (a real 0 that was sent)\n", v, ok)
	v, ok = <-ch
	fmt.Printf("  v=%d ok=%v (closed, zero value)\n", v, ok)

	fmt.Println("\n== 3. buffered values survive close ==")
	buf := make(chan string, 3)
	buf <- "x"
	buf <- "y"
	close(buf)
	for s := range buf {
		fmt.Println(" ", s) // still gets x and y
	}

	fmt.Println("\n== 4. close as a broadcast to many goroutines ==")
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Go(func() {
			<-start // all block here...
			fmt.Println("  runner", i, "go!")
		})
	}
	time.Sleep(20 * time.Millisecond)
	close(start) // ...and all wake up at once. A send would wake only one.
	wg.Wait()

	fmt.Println("\n== 5. panics (recovered so you can see them) ==")
	tryPanic("send on closed", func() { c := make(chan int); close(c); c <- 1 })
	tryPanic("close twice", func() { c := make(chan int); close(c); close(c) })
	tryPanic("close nil", func() { var c chan int; close(c) })
}

func tryPanic(name string, f func()) {
	defer func() { fmt.Printf("  %-15s -> panic: %v\n", name, recover()) }()
	f()
}

// Try it:
//   1. In part 1 remove `defer close(nums)`. What error do you get and why?
//   2. Write a generator `func count(n int) <-chan int` that sends 0..n-1 then closes.
