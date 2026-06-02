package network

import (
	"bufio"
	"net"
	"strings"

	"minikv/internal/gossip"
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
	g *gossip.Gossip,
) {

	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {

		message, err := reader.ReadString('\n')

		if err != nil {
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

		_, err = writer.WriteString(response + "\n")

		if err != nil {
			return
		}

		err = writer.Flush()

		if err != nil {
			return
		}
	}
}
