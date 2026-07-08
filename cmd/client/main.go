package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run cmd/client/main.go <port>")
		return
	}

	address := "localhost:" + os.Args[1]

	conn, err := net.Dial("tcp", address)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Connected to", address)

	reader := bufio.NewReader(os.Stdin)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print("> ")

		cmd, _ := reader.ReadString('\n')
		conn.Write([]byte(cmd))

		response, _ := serverReader.ReadString('\n')
		fmt.Println(response)
	}
}