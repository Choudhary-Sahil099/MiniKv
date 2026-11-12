package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

type Result struct {
	successes int
	failures  int
	latencies []time.Duration
}

func worker(
	workerID int,
	address string,
	requests int,
	timeout time.Duration,
	ready *sync.WaitGroup,
	start <-chan struct{},
	results chan<- Result,
) {
	result := Result{}
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		result.failures = requests
		ready.Done()
		results <- result
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	ready.Done()
	<-start

	for i := 0; i < requests; i++ {
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			result.failures += requests - i
			break
		}
		key := fmt.Sprintf("bench_%d_%d", workerID, i)
		command := fmt.Sprintf("SET %s value_%d\n", key, i)
		requestStart := time.Now()
		if _, err := writer.WriteString(command); err != nil {
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

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

func main() {
	requests := flag.Int("requests", 10000, "total SET requests")
	concurrency := flag.Int("concurrency", 100, "number of concurrent client connections")
	addressesFlag := flag.String("addresses", "127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002", "comma-separated MiniKV node addresses")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request network timeout")
	flag.Parse()

	if *requests <= 0 || *concurrency <= 0 || *timeout <= 0 {
		fmt.Println("requests, concurrency, and timeout must be positive")
		return
	}
	addresses := make([]string, 0)
	for _, address := range strings.Split(*addressesFlag, ",") {
		address = strings.TrimSpace(address)
		if address != "" {
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		fmt.Println("at least one node address is required")
		return
	}

	results := make(chan Result, *concurrency)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(*concurrency)
	requestsPerWorker := *requests / *concurrency
	remainder := *requests % *concurrency

	for workerID := 0; workerID < *concurrency; workerID++ {
		workerRequests := requestsPerWorker
		if workerID < remainder {
			workerRequests++
		}
		address := addresses[workerID%len(addresses)]
		go worker(workerID, address, workerRequests, *timeout, &ready, start, results)
	}

	ready.Wait()
	begin := time.Now()
	close(start)

	var successes, failures int
	latencies := make([]time.Duration, 0, *requests)
	for i := 0; i < *concurrency; i++ {
		result := <-results
		successes += result.successes
		failures += result.failures
		latencies = append(latencies, result.latencies...)
	}
	duration := time.Since(begin)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Println("Workload: SET-only, persistent TCP connections")
	fmt.Println("Node addresses:", strings.Join(addresses, ","))
	fmt.Println("Target requests:", *requests)
	fmt.Println("Concurrent clients:", *concurrency)
	fmt.Println("Successful writes:", successes)
	fmt.Println("Failed writes:", failures)
	fmt.Printf("Duration: %v\n", duration)
	if duration > 0 {
		fmt.Printf("Successful writes/sec: %.2f\n", float64(successes)/duration.Seconds())
	}
	fmt.Println("Latency samples:", len(latencies))
	if len(latencies) > 0 {
		fmt.Printf("p50 latency: %v\n", percentile(latencies, 0.50))
		fmt.Printf("p95 latency: %v\n", percentile(latencies, 0.95))
		fmt.Printf("p99 latency: %v\n", percentile(latencies, 0.99))
	}
}
