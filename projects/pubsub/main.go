// Project: in-memory pub/sub broker
//
// Publishers send messages to a topic; every subscriber of that topic gets a
// copy. Subscribers can join and leave at any time.
//
// Design: the ACTOR pattern. One goroutine (run) owns all broker state. Every
// operation (subscribe, unsubscribe, publish, stop) is a message sent to it.
// No mutex anywhere, because only one goroutine ever touches the map.
//
// Hard part: a slow subscriber must not stall the broker for everyone. Each
// subscriber gets a buffered channel, and the broker does a non-blocking send
// (select + default). If the buffer is full, the message is dropped for that
// subscriber only. Real systems pick a policy: drop, disconnect, or block.
//
// Concepts combined: select (06), close as broadcast (04), buffered channels
// (03), ownership by a single goroutine (08), clean shutdown (12, 13).
//
// Run: go run ./projects/pubsub
package main

import (
	"fmt"
	"sync"
	"time"
)

type Message struct {
	Topic string
	Body  string
}

type subscription struct {
	topic string
	ch    chan Message
}

type Broker struct {
	subscribe   chan subscription
	unsubscribe chan subscription
	publish     chan Message
	stop        chan struct{}
	stopped     chan struct{}
}

func NewBroker() *Broker {
	b := &Broker{
		subscribe:   make(chan subscription),
		unsubscribe: make(chan subscription),
		publish:     make(chan Message),
		stop:        make(chan struct{}),
		stopped:     make(chan struct{}),
	}
	go b.run()
	return b
}

// run is the only goroutine that reads or writes subs.
func (b *Broker) run() {
	defer close(b.stopped)
	subs := map[string]map[chan Message]bool{}
	dropped := 0

	for {
		select {
		case s := <-b.subscribe:
			if subs[s.topic] == nil {
				subs[s.topic] = map[chan Message]bool{}
			}
			subs[s.topic][s.ch] = true

		case s := <-b.unsubscribe:
			if subs[s.topic][s.ch] {
				delete(subs[s.topic], s.ch)
				close(s.ch) // the broker is the sender, so the broker closes
			}

		case m := <-b.publish:
			for ch := range subs[m.Topic] {
				select {
				case ch <- m:
				default:
					dropped++ // subscriber too slow, don't block everyone else
				}
			}

		case <-b.stop:
			for _, set := range subs {
				for ch := range set {
					close(ch) // ends every subscriber's `for range`
				}
			}
			fmt.Printf("broker: stopped, dropped %d messages for slow subscribers\n", dropped)
			return
		}
	}
}

// Subscribe returns a receive-only channel of messages and a function to leave.
func (b *Broker) Subscribe(topic string, buffer int) (<-chan Message, func()) {
	s := subscription{topic: topic, ch: make(chan Message, buffer)}
	b.subscribe <- s
	return s.ch, func() { b.unsubscribe <- s }
}

func (b *Broker) Publish(topic, body string) { b.publish <- Message{topic, body} }

// Stop shuts the broker down and waits until it has closed all subscribers.
func (b *Broker) Stop() {
	close(b.stop)
	<-b.stopped
}

func main() {
	b := NewBroker()
	var wg sync.WaitGroup

	listen := func(name, topic string, buffer int, delay time.Duration) {
		msgs, _ := b.Subscribe(topic, buffer)
		wg.Go(func() {
			got := 0
			for m := range msgs {
				got++
				fmt.Printf("  %-6s <- [%s] %s\n", name, m.Topic, m.Body)
				time.Sleep(delay)
			}
			fmt.Printf("  %-6s done, received %d\n", name, got)
		})
	}

	listen("alice", "orders", 10, 0)
	listen("bob", "orders", 10, 0)
	listen("slow", "orders", 1, 50*time.Millisecond) // will miss messages
	listen("carol", "users", 10, 0)

	// A subscriber that leaves early.
	temp, leave := b.Subscribe("orders", 10)
	b.Publish("orders", "order #1")
	m := <-temp
	fmt.Printf("  temp   <- [%s] %s, then unsubscribes\n", m.Topic, m.Body)
	leave()

	for i := 2; i <= 5; i++ {
		b.Publish("orders", fmt.Sprintf("order #%d", i))
	}
	b.Publish("users", "user signed up")

	time.Sleep(100 * time.Millisecond) // let fast subscribers drain
	b.Stop()
	wg.Wait()
}

// Try it:
//   1. Change the slow subscriber's policy: instead of dropping, unsubscribe
//      (disconnect) it when its buffer is full.
//   2. Rewrite the broker with a sync.RWMutex instead of the actor goroutine.
//      Compare: which one makes "publish while someone unsubscribes" obviously safe?
