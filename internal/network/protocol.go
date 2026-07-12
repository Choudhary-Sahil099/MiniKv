package network

import (
	"encoding/json"
	"go.uber.org/zap"
	"minikv/internal/gossip"
	"minikv/internal/handoff"
	"minikv/internal/hashring"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/network/handlers"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"strings"
	"time"
)

func ProcessCommand(
	command string,
	nodeID string,
	store *storage.Store,
	wal *wal.WAL,
	ring *hashring.HashRing,
	isForwarded bool,
	g *gossip.Gossip,
	handoffManager *handoff.Manager,
) string {

	start := time.Now()

	defer func() {
		metrics.RequestLatency.Observe(
			time.Since(start).Seconds(),
		)
	}()

	metrics.TotalRequests.
		WithLabelValues(nodeID).
		Inc()

	logger.Log.Info(
		"processing command",
		zap.String("command", command),
	)
	parts := strings.Fields(command)

	if len(parts) == 0 {
		return "Empty command"
	}

	switch strings.ToUpper(parts[0]) {

	case "SET":

		return handlers.HandleSET(
			command,
			nodeID,
			store,
			wal,
			ring,
			isForwarded,
			g,
			handoffManager,
		)

	case "GET":

		return handlers.HandleGET(
			command,
			nodeID,
			store,
			wal,
			ring,
			isForwarded,
			g,
		)

	case "DEL":

		return handlers.HandleDEL(
			command,
			nodeID,
			store,
			wal,
			ring,
			isForwarded,
			g,
		)
	case "REPL_DEL":

		return handlers.HandleReplDelete(
			command,
			store,
			wal,
		)
	case "REPL_SET":

		return handlers.HandleReplSet(
			command,
			nodeID,
			store,
			wal,
		)

	case "DUMP":

		data := store.Export()

		bytes, err := json.Marshal(data)

		if err != nil {

			logger.Log.Error(
				"dump marshal failed",
				zap.Error(err),
			)

			return "DUMP_FAILED"
		}

		return string(bytes) + "\n"

	case "LOCAL_GET":

		return handlers.HandleLocalGet(
			command,
			store,
		)

	case "LOCAL_GET_VALUE":

		if len(parts) != 2 {
			return "Usage: LOCAL_GET_VALUE key"
		}

		value, exists := store.GetValue(parts[1])

		if !exists {
			return "NOT_FOUND"
		}

		bytes, err := json.Marshal(value)

		if err != nil {
			return "ERROR"
		}

		return string(bytes)

	case "PING":

		return "PONG"

	default:

		logger.Log.Warn(
			"unknown command received",
			zap.String("command", command),
		)

		return "Unknown command"
	}
}
