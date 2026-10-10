
package main

import (
	"bufio"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	totalRequests = 10000
	concurrency   = 100
	serverAddress = "localhost:5000"
)

type Result struct {
	successes int
	failures  int
	latencies []time.Duration
}

func worker(
	workerID int,
	requests int,
	ready *sync.WaitGroup,
	start <-chan struct{},
	results chan<- Result,
) {
	result := Result{}

	conn, err := net.DialTimeout(
		"tcp", serverAddress, 5*time.Second,
	)
	if err != nil {
		result.failures = requests
		ready.Done()
		results <- result
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Wait until all workers have attempted to connect.
	ready.Done()
	<-start

	for i := 0; i < requests; i++ {
		cmd := fmt.Sprintf(
			"SET key%d_%d value%d\n",
			workerID, i, i,
		)

		requestStart := time.Now()

		if _, err := writer.WriteString(cmd); err != nil {
			result.failures += requests - i
			break
		}

		if err := writer.Flush(); err != nil {
			result.failures += requests - i
			break
		}

		response, err := reader.ReadString('\n')
		elapsed := time.Since(requestStart)

		if err != nil {
			result.failures += requests - i
			break
		}

		if strings.TrimSpace(response) != "OK" {
			result.failures++
			continue
		}

		result.successes++
		result.latencies = append(result.latencies, elapsed)
	}

	results <- result
}

func percentile(
	sorted []time.Duration,
	p float64,
) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

func main() {
	results := make(chan Result, concurrency)
	start := make(chan struct{})

	var ready sync.WaitGroup
	ready.Add(concurrency)

	requestsPerWorker := totalRequests / concurrency
	remainder := totalRequests % concurrency

	for i := 0; i < concurrency; i++ {
		requests := requestsPerWorker
		if i < remainder {
			requests++
		}

		go worker(
			i,
			requests,
			&ready,
			start,
			results,
		)
	}

	ready.Wait()

	begin := time.Now()
	close(start)

	var successes, failures int
	var latencies []time.Duration

	for i := 0; i < concurrency; i++ {
		result := <-results
		successes += result.successes
		failures += result.failures
		latencies = append(latencies, result.latencies...)
	}

	duration := time.Since(begin)

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	fmt.Println("Target requests:", totalRequests)
	fmt.Println("Successful writes:", successes)
	fmt.Println("Failed writes:", failures)
	fmt.Printf("Duration: %v\n", duration)

	if duration > 0 {
		fmt.Printf(
			"Successful writes/sec: %.2f\n",
			float64(successes)/duration.Seconds(),
		)
	}

	fmt.Println("Latency samples:", len(latencies))

	if len(latencies) > 0 {
		fmt.Printf("p50 latency: %v\n",
			percentile(latencies, 0.50))
		fmt.Printf("p95 latency: %v\n",
			percentile(latencies, 0.95))
		fmt.Printf("p99 latency: %v\n",
			percentile(latencies, 0.99))
	}
}
