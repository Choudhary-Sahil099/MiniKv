package network

import (
	"go.uber.org/zap"
	"minikv/internal/gossip"
	"minikv/internal/handoff"
	"minikv/internal/hashring"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/network/common"
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
	ctx := &common.CommandContext{
		NodeID:         nodeID,
		Store:          store,
		WAL:            wal,
		Ring:           ring,
		Gossip:         g,
		HandoffManager: handoffManager,
		IsForwarded:    isForwarded,
	}
	switch strings.ToUpper(parts[0]) {

	case "SET":

		return handlers.HandleSET(
			ctx,
			command,
		)

	case "GET":

		return handlers.HandleGET(
			ctx,
			command,
		)

	case "DEL":

		return handlers.HandleDEL(
			ctx,
			command,
		)
	case "REPL_DEL":

		return handlers.HandleReplDelete(
			ctx,
			command,
		)
	case "REPL_SET":

		return handlers.HandleReplSet(
			ctx,
			command,
		)

	case "DUMP":

		return handlers.HandleDump(ctx)

	case "MERKLE_ROOT":

		return handlers.HandleMerkleRoot(ctx)

	case "LOCAL_GET":
		
		return handlers.HandleLocalGet(
			ctx,
			command,
		)

	case "LOCAL_GET_VALUE":

		return handlers.HandleLocalGetValue(
			ctx,
			command,
		)

	case "PING":

		return handlers.HandlePing(ctx)

	default:

		logger.Log.Warn(
			"unknown command received",
			zap.String("command", command),
		)

		return "Unknown command"
	}
}
