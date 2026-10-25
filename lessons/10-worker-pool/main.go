// Lesson 10: Worker pool
//
// Fixed number of workers pulling jobs from a shared channel. This is the
// most common concurrency pattern in real Go services: it bounds concurrency
// (don't open 10_000 DB connections) and reuses goroutines.
//
//	           ┌-> worker 1 ─┐
//	jobs ──────┼-> worker 2 ─┼──> results
//	           └-> worker 3 ─┘
//
// Shutdown sequence (the part people get wrong):
//  1. producer closes `jobs` when all jobs are sent
//  2. each worker's `for range jobs` ends
//  3. after ALL workers exit (WaitGroup), close `results`
//  4. the consumer's `for range results` ends
//
// Run: go run ./lessons/10-worker-pool
package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type Job struct {
	ID  int
	URL string
}

type Result struct {
	JobID    int
	WorkerID int
	Status   int
	Took     time.Duration
}

func worker(id int, jobs <-chan Job, results chan<- Result) {
	for j := range jobs {
		start := time.Now()
		time.Sleep(time.Duration(10+rand.IntN(40)) * time.Millisecond) // pretend HTTP call
		results <- Result{JobID: j.ID, WorkerID: id, Status: 200, Took: time.Since(start)}
	}
}

func main() {
	const numWorkers = 3
	const numJobs = 10

	jobs := make(chan Job)
	results := make(chan Result)

	var wg sync.WaitGroup
	for w := 1; w <= numWorkers; w++ {
		wg.Go(func() { worker(w, jobs, results) })
	}

	// Producer in its own goroutine, so main can read results concurrently.
	// If main sent all jobs first, it would block: workers would be stuck
	// sending results nobody reads.
	go func() {
		defer close(jobs) // step 1
		for i := 1; i <= numJobs; i++ {
			jobs <- Job{ID: i, URL: fmt.Sprintf("https://example.com/%d", i)}
		}
	}()

	go func() {
		wg.Wait()      // step 3
		close(results) // nobody else closes results: workers don't know if they're last
	}()

	perWorker := map[int]int{}
	start := time.Now()
	for r := range results { // step 4
		fmt.Printf("job %2d done by worker %d in %v\n", r.JobID, r.WorkerID, r.Took.Round(time.Millisecond))
		perWorker[r.WorkerID]++
	}
	fmt.Printf("\nall done in %v, jobs per worker: %v\n", time.Since(start).Round(time.Millisecond), perWorker)
}

// Try it:
//   1. Change numWorkers to 1 and 10. How does total time change?
//   2. Make a worker return an error for job 7 (add Err to Result) and print it.
//   3. Have main send jobs directly (no producer goroutine) with unbuffered
//      channels. Why does it deadlock? Fix it by buffering results instead.
