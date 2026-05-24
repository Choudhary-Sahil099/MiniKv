package network

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"minikv/internal/hashring"
	"minikv/internal/storage"
	"minikv/internal/wal"
)

func HandleConnection(
	conn net.Conn,
	nodeID string,
	store *storage.Store,
	wal *wal.WAL,
	ring *hashring.HashRing,
) {

	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {

		message, err := reader.ReadString('\n')

		if err != nil {
			fmt.Println("Client disconnected")
			return
		}
		isForwarded := false

		if strings.HasPrefix(message, "FORWARDED ") {

			isForwarded = true

			message = strings.TrimPrefix(
				message,
				"FORWARDED ",
			)
		}

		response := ProcessCommand(
			message,
			nodeID,
			store,
			wal,
			ring,
			isForwarded,
		)

		conn.Write([]byte(response + "\n"))
	}
}
