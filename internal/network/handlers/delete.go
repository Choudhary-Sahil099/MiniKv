package handlers

import (
	"go.uber.org/zap"
	"minikv/internal/client"
	"minikv/internal/gossip"
	"minikv/internal/hashring"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"strings"
)

func HandleDEL(
	command string,
	nodeID string,
	store *storage.Store,
	wal *wal.WAL,
	ring *hashring.HashRing,
	isForwarded bool,
	g *gossip.Gossip,
) string {

	parts := strings.Fields(command)

	if len(parts) != 2 {
		return "Usage: DEL key"
	}

	metrics.DelRequests.Inc()

	if len(parts) != 2 {
		return "Usage: DEL key"
	}

	owner := ring.GetNode(parts[1])

	if !g.IsAlive(owner.ID) {

		logger.Log.Warn(
			"using replica due to node failure",
			zap.String("failed_node", owner.ID),
		)

		owner = ring.GetReplicaNode(parts[1])
	}

	if !isForwarded && owner.ID != nodeID {

		metrics.ForwardedRequests.Inc()

		response, err := client.ForwardCommand(
			owner.Address,
			command,
		)

		if err != nil {

			logger.Log.Error(
				"forward failed",
				zap.String("target", owner.ID),
				zap.Error(err),
			)

			return "Forwarding failed"
		}

		return response
	}

	err := wal.Write(command)

	if err != nil {

		logger.Log.Error(
			"wal write failed",
			zap.Error(err),
		)

		return "WAL write failed"
	}

	store.Delete(parts[1])
	replicas := ring.GetReplicaNodes(parts[1])

	for _, replica := range replicas {

		if replica.ID == nodeID {
			continue
		}

		metrics.ReplicationRequests.Inc()

		_, err := client.ForwardCommand(
			replica.Address,
			"REPL_DEL "+parts[1],
		)

		if err != nil {

			logger.Log.Warn(
				"delete replication failed",
				zap.String("replica", replica.ID),
				zap.Error(err),
			)

			continue
		}
	}
	return "Deleted"
}
