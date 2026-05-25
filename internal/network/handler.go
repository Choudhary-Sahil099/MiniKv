package network

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"minikv/internal/hashring"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"minikv/internal/gossip"
)

func HandleConnection(
	conn net.Conn,
	nodeID string,
	store *storage.Store,
	wal *wal.WAL,
	ring *hashring.HashRing,
	g *gossip.Gossip,
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
			g,
		)

		conn.Write([]byte(response + "\n"))
	}
}
