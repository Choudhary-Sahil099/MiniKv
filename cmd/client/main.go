package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {

	conn, err := net.Dial(
		"tcp",  
		"localhost:5002", // change to 5001/5000 when changing the dump values or replication checking
	)

	if err != nil {
		panic(err)
	}

	defer conn.Close()

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