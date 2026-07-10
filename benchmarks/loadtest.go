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
	workerID int,
) {

	defer wg.Done()

	conn, err := net.Dial(
		"tcp",
		"localhost:5000",
	)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for i := 0; i < requests; i++ {

		cmd := fmt.Sprintf(
			"SET key%d_%d value%d\n",
			workerID,
			i,
			i,
		)

		_, err := writer.WriteString(cmd)

		if err != nil {
			fmt.Println(err)
			return
		}

		// Flush buffered data
		err = writer.Flush()

		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = reader.ReadString('\n')

		if err != nil {
			fmt.Println(err)
			return
		}
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
			i,
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
