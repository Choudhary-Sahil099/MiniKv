package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	totalRequests = 10000
	concurrency   = 100
)

func worker(
	wg *sync.WaitGroup,
	requests int,
) {

	defer wg.Done()

	for i := 0; i < requests; i++ {

		conn, err := net.Dial(
			"tcp",
			"localhost:5000",
		)

		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Fprintf(
			conn,
			"SET key%d value%d\n",
			i,
			i,
		)

		bufio.NewReader(conn).
			ReadString('\n')

		conn.Close()
	}
}

func main() {

	start := time.Now()

	var wg sync.WaitGroup

	requestsPerWorker :=
		totalRequests / concurrency

	for i := 0; i < concurrency; i++ {

		wg.Add(1)

		go worker(
			&wg,
			requestsPerWorker,
		)
	}

	wg.Wait()

	duration := time.Since(start)

	fmt.Println(
		"Total Requests:",
		totalRequests,
	)

	fmt.Println(
		"Duration:",
		duration,
	)

	fmt.Println(
		"Requests/sec:",
		float64(totalRequests)/
			duration.Seconds(),
	)
}