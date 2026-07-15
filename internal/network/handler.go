package network

import (
	"bufio"
	"net"
	"strings"

	"go.uber.org/zap"
	"minikv/internal/gossip"
	"minikv/internal/handoff"
	"minikv/internal/hashring"
	"minikv/internal/logger"
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
	handoff *handoff.Manager,
) {

	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {

		message, err := reader.ReadString('\n')

		if err != nil {
			return
		}
		logger.Log.Info(
			"received raw message",
			zap.String("message", message),
		)
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
			handoff,
		)
		logger.Log.Info(
			"sending response",
			zap.String("response", response),
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
