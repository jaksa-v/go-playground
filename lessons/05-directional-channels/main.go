// Lesson 05: Directional channel types
//
//	chan T     bidirectional
//	chan<- T   send-only    ("arrow points into the chan")
//	<-chan T   receive-only ("arrow points out of the chan")
//
// A bidirectional channel converts implicitly to either direction, never back.
// Use directions in function signatures to document ownership and let the
// compiler enforce it: a function taking <-chan T can't send on it or close it.
//
// Idiom: the function that CREATES a channel owns it, writes to it, closes it,
// and returns it as <-chan T. Callers can only read.
//
// Run: go run ./lessons/05-directional-channels
package main

import "fmt"

// producer owns the channel: creates, writes, closes. Returns read-only.
func producer(words ...string) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		for _, w := range words {
			out <- w
		}
	}()
	return out
}

// shout can only read from in and only write to out.
func shout(in <-chan string, out chan<- string) {
	defer close(out)
	for w := range in {
		out <- w + "!"
	}
	// in <- "x"   // compile error: send to receive-only channel
	// close(in)   // compile error: close of receive-only channel
}

func main() {
	words := producer("go", "is", "fun")
	loud := make(chan string)
	go shout(words, loud) // chan string converts to chan<- string automatically

	for w := range loud {
		fmt.Println(w)
	}
}

// Try it:
//   1. Uncomment the two lines in shout and read the compiler errors.
//   2. Try `var c chan int = producer(...)`. Why does it fail to compile?
