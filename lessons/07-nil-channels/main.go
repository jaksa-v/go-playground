// Lesson 07: nil channels, the select on/off switch
//
// Sending to or receiving from a nil channel blocks forever. Sounds useless,
// but inside select a case on a nil channel is simply never chosen. So setting
// a channel variable to nil DISABLES that case.
//
// Classic use: merging two channels and stopping when BOTH are closed.
//
// Run: go run ./lessons/07-nil-channels
package main

import "fmt"

func gen(prefix string, n int) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		for i := range n {
			out <- fmt.Sprintf("%s%d", prefix, i)
		}
	}()
	return out
}

func main() {
	a, b := gen("a", 2), gen("b", 4)

	// Naive version bug: once `a` is closed, `<-a` is always ready and returns
	// "" forever, spinning the loop. Nil-ing it out removes it from the select.
	for a != nil || b != nil {
		select {
		case v, ok := <-a:
			if !ok {
				fmt.Println("a closed")
				a = nil
				continue
			}
			fmt.Println(v)
		case v, ok := <-b:
			if !ok {
				fmt.Println("b closed")
				b = nil
				continue
			}
			fmt.Println(v)
		}
	}
	fmt.Println("both done")
}

// Try it:
//   1. Remove the `a = nil` line and add a counter. How many times does the loop spin?
//   2. Use the same trick to pause a producer: set `out = nil` when a buffer is
//      full and restore it when there's room.
