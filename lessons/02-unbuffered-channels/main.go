// Lesson 02: Unbuffered channels
//
// A channel is a typed pipe goroutines use to communicate:
//
//	ch := make(chan int) // unbuffered
//	ch <- 42             // send
//	v := <-ch            // receive
//
// An UNBUFFERED channel has no storage. A send blocks until another goroutine
// receives, and a receive blocks until another goroutine sends. Both sides meet
// at the same moment (a "rendezvous"). That makes it both a data transfer AND
// a synchronization point.
//
// Go's motto: "Don't communicate by sharing memory; share memory by communicating."
//
// Run: go run ./lessons/02-unbuffered-channels
package main

import (
	"fmt"
	"time"
)

func main() {
	fmt.Println("== 1. basic send/receive ==")
	ch := make(chan string)
	go func() {
		ch <- "hello from goroutine" // blocks until main receives
	}()
	msg := <-ch // blocks until the goroutine sends
	fmt.Println(msg)

	fmt.Println("\n== 2. channel as a 'done' signal (replaces WaitGroup for one goroutine) ==")
	done := make(chan struct{}) // struct{} carries no data, costs zero bytes
	go func() {
		fmt.Println("working...")
		time.Sleep(50 * time.Millisecond)
		done <- struct{}{}
	}()
	<-done
	fmt.Println("worker finished")

	fmt.Println("\n== 3. the sender blocks until someone receives ==")
	ch2 := make(chan int)
	go func() {
		start := time.Now()
		ch2 <- 1
		fmt.Printf("sender unblocked after %v\n", time.Since(start).Round(10*time.Millisecond))
	}()
	time.Sleep(100 * time.Millisecond) // the sender is stuck the whole time
	<-ch2
	time.Sleep(10 * time.Millisecond) // give the sender time to print

	fmt.Println("\n== 4. returning a result from a goroutine ==")
	result := make(chan int)
	go func() { result <- slowSquare(7) }()
	fmt.Println("doing other work while waiting...")
	fmt.Println("7^2 =", <-result)

	fmt.Println("\n== 5. ping-pong: two goroutines taking turns ==")
	ping, pong := make(chan int), make(chan int)
	go func() {
		for v := range 3 {
			ping <- v
			fmt.Println("  got back", <-pong)
		}
		close(ping)
	}()
	for v := range ping { // range over a channel stops when it is closed (lesson 05)
		fmt.Println("  received", v)
		pong <- v * 10
	}
}

func slowSquare(n int) int {
	time.Sleep(50 * time.Millisecond)
	return n * n
}

// Try it:
//   1. Delete the `go` in part 1 so the send runs in main. You get
//      "fatal error: all goroutines are asleep - deadlock!". Why?
//   2. Send two values in part 1 but receive only one. Does the program hang? Why not?
//      (Hint: main exits. That stuck goroutine is a "leak", see lesson 13.)
