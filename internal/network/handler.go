package network

import (
	"bufio"
	"fmt"
	"net"
)
import "minikv/internal/storage"
import "minikv/internal/wal"
func HandleConnection(
	conn net.Conn,
	store *storage.Store,
	wal *wal.WAL,
) {

	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {

		message, err := reader.ReadString('\n')

		if err != nil {
			fmt.Println("Client disconnected")
			return
		}

		response := ProcessCommand(message, store, wal)

		conn.Write([]byte(response + "\n"))
	}
}
