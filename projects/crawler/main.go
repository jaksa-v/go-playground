// Project: concurrent web crawler
//
// Crawl a site graph starting from one URL, following links up to a depth,
// fetching each URL exactly once, with at most N fetches in flight, and a
// global timeout. Uses a fake in-memory "internet" so it runs offline.
//
// Concepts combined:
//   - one goroutine per URL, bounded by a semaphore      (lessons 01, 11)
//   - a mutex-guarded "seen" set for dedup               (lesson 08)
//   - WaitGroup to know when the whole crawl is finished (lesson 01)
//   - results streamed over a channel, closed after Wait (lessons 04, 10)
//   - per-page and whole-crawl context timeouts          (lesson 12)
//
// Run: go run ./projects/crawler
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Page struct {
	URL   string
	Depth int
	Links []string
	Err   error
}

// fakeWeb maps URL -> links on that page, plus how long the "server" takes.
var fakeWeb = map[string]struct {
	links   []string
	latency time.Duration
}{
	"https://go.dev/":                 {[]string{"https://go.dev/doc/", "https://go.dev/blog/", "https://pkg.go.dev/"}, 30 * time.Millisecond},
	"https://go.dev/doc/":             {[]string{"https://go.dev/", "https://go.dev/doc/effective_go", "https://go.dev/ref/mem"}, 40 * time.Millisecond},
	"https://go.dev/blog/":            {[]string{"https://go.dev/", "https://go.dev/blog/pipelines"}, 20 * time.Millisecond},
	"https://go.dev/blog/pipelines":   {[]string{"https://go.dev/blog/", "https://go.dev/ref/mem"}, 50 * time.Millisecond},
	"https://go.dev/doc/effective_go": {[]string{"https://go.dev/doc/"}, 30 * time.Millisecond},
	"https://go.dev/ref/mem":          {[]string{"https://go.dev/doc/"}, 25 * time.Millisecond},
	"https://pkg.go.dev/":             {[]string{"https://pkg.go.dev/sync", "https://go.dev/"}, 35 * time.Millisecond},
	"https://pkg.go.dev/sync":         {[]string{"https://pkg.go.dev/"}, 2 * time.Second}, // slow, will time out
}

func fetch(ctx context.Context, url string) ([]string, error) {
	page, ok := fakeWeb[url]
	if !ok {
		return nil, fmt.Errorf("404 not found")
	}
	select {
	case <-time.After(page.latency):
		return page.links, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type Crawler struct {
	maxDepth int
	sem      chan struct{} // bounds concurrent fetches
	results  chan Page

	mu   sync.Mutex
	seen map[string]bool
	wg   sync.WaitGroup
}

// Crawl starts crawling and returns a channel of pages that is closed when done.
func Crawl(ctx context.Context, start string, maxDepth, maxConcurrent int) <-chan Page {
	c := &Crawler{
		maxDepth: maxDepth,
		sem:      make(chan struct{}, maxConcurrent),
		results:  make(chan Page),
		seen:     map[string]bool{},
	}
	c.visit(ctx, start, 0)
	go func() {
		c.wg.Wait() // every visit has finished, including ones spawned by others
		close(c.results)
	}()
	return c.results
}

// visit claims url (if unseen) and crawls it in a new goroutine.
// wg.Add happens here, before the goroutine starts and while the parent visit
// is still counted, so the WaitGroup can't hit zero early.
func (c *Crawler) visit(ctx context.Context, url string, depth int) {
	if depth > c.maxDepth {
		return
	}
	c.mu.Lock()
	if c.seen[url] {
		c.mu.Unlock()
		return
	}
	c.seen[url] = true
	c.mu.Unlock()

	c.wg.Go(func() {
		select {
		case c.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		// Per-page timeout as a child of the crawl context: a slow page fails
		// on its own, while cancelling the crawl still stops every fetch.
		pageCtx, cancel := context.WithTimeout(ctx, pageTimeout)
		links, err := fetch(pageCtx, url)
		cancel()
		<-c.sem // release before sending, so a slow consumer doesn't hold a slot

		select {
		case c.results <- Page{URL: url, Depth: depth, Links: links, Err: err}:
		case <-ctx.Done():
			return
		}
		for _, l := range links {
			c.visit(ctx, l, depth+1)
		}
	})
}

const pageTimeout = 200 * time.Millisecond

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second) // whole crawl
	defer cancel()

	start := time.Now()
	n := 0
	for p := range Crawl(ctx, "https://go.dev/", 3, 3) {
		n++
		if p.Err != nil {
			fmt.Printf("[d%d] %-34s ERROR %v\n", p.Depth, p.URL, p.Err)
			continue
		}
		fmt.Printf("[d%d] %-34s %d links\n", p.Depth, p.URL, len(p.Links))
	}
	fmt.Printf("\ncrawled %d pages in %v\n", n, time.Since(start).Round(time.Millisecond))
}

// Try it:
//   1. Set maxConcurrent to 1. Compare total time.
//   2. Raise pageTimeout to 3s. pkg.go.dev/sync now gets cut off by the crawl
//      timeout instead, and its page is silently dropped. Why? (look at the
//      second select in visit)
//   3. Replace the mutex + seen map with a single "coordinator" goroutine that
//      owns `seen` and receives discovered links over a channel. Which version
//      is easier to read?
