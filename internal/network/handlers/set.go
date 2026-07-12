package handlers

import (
	"go.uber.org/zap"
	"minikv/internal/client"
	"minikv/internal/config"
	"minikv/internal/gossip"
	"minikv/internal/handoff"
	"minikv/internal/hashring"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"time"
	"strings"
)

func HandleSET(
	command string,
	nodeID string,
	store *storage.Store,
	wal *wal.WAL,
	ring *hashring.HashRing,
	isForwarded bool,
	g *gossip.Gossip,
	handoffManager *handoff.Manager,
) string {
	parts := strings.Fields(command)

	if len(parts) != 3 {
		return "Usage: SET key value"
	}
	metrics.SetRequests.
		WithLabelValues(nodeID).
		Inc()

	if len(parts) != 3 {
		return "Usage: SET key value"
	}

	owner := ring.GetNode(parts[1])

	replicas := ring.GetReplicaNodes(parts[1])

	replicaIDs := []string{}
	for _, replica := range replicas {
		replicaIDs = append(replicaIDs, replica.ID)
	}

	logger.Log.Info(
		"placement",
		zap.String("key", parts[1]),
		zap.String("owner", owner.ID),
		zap.Strings("replicas", replicaIDs),
	)

	if !g.IsAlive(owner.ID) {

		logger.Log.Warn(
			"using replica due to node failure",
			zap.String("failed_node", owner.ID),
		)

		owner = ring.GetReplicaNode(parts[1])
	}

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

	store.Set(parts[1], parts[2])

	storedValue, _ := store.GetValue(parts[1])

	// Owner has already stored the write.
	successfulWrites := 1

	for _, replica := range replicas {

		if replica.ID == nodeID {
			continue
		}

		metrics.ReplicationRequests.Inc()
		replicationCommand :=
			"REPL_SET " +
				parts[1] + " " +
				parts[2] + " " +
				storedValue.CreatedAt.Format(time.RFC3339Nano)
		response, err := client.ForwardCommand(
			replica.Address,
			replicationCommand,
		)

		if err != nil {

			logger.Log.Warn(
				"replication failed",
				zap.String("replica", replica.ID),
				zap.Error(err),
			)
			handoffManager.AddHint(
				handoff.Hint{
					TargetNode: replica.ID,
					Command:    replicationCommand,
					CreatedAt:  time.Now(),
				},
			)

			continue
		}

		if response == "REPLICATED" {

			successfulWrites++

			logger.Log.Info(
				"replica acknowledged write",
				zap.String("replica", replica.ID),
				zap.Int("acks", successfulWrites),
			)
		}
	}
	if successfulWrites >= config.WriteQuorum {

		logger.Log.Info(
			"write quorum achieved",
			zap.Int("acks", successfulWrites),
		)

		return "OK"
	}

	logger.Log.Error(
		"write quorum failed",
		zap.Int("acks", successfulWrites),
	)

	return "WRITE_QUORUM_FAILED"

}
