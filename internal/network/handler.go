package network

import (
	"bufio"
	"fmt"
	"net"
)
import "minikv/internal/storage"

func HandleConnection(conn net.Conn, store *storage.Store) {

	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {

		message, err := reader.ReadString('\n')

		if err != nil {
			fmt.Println("Client disconnected")
			return
		}

		response := ProcessCommand(message, store)

		conn.Write([]byte(response + "\n"))
	}
}
