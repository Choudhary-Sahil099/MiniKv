package handlers

import (
	"go.uber.org/zap"
	"minikv/internal/client"
	"minikv/internal/config"
	"minikv/internal/handoff"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/network/common"
	"time"
	"strings"
)

func HandleSET(
	ctx *common.CommandContext,
	command string,
) string {
	parts := strings.Fields(command)

	if len(parts) != 3 {
		return "Usage: SET key value"
	}
	metrics.SetRequests.
		WithLabelValues(ctx.NodeID).
		Inc()

	if len(parts) != 3 {
		return "Usage: SET key value"
	}

	owner := ctx.Ring.GetNode(parts[1])

	replicas := ctx.Ring.GetReplicaNodes(parts[1])

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

	if !ctx.Gossip.IsAlive(owner.ID) {

		logger.Log.Warn(
			"using replica due to node failure",
			zap.String("failed_node", owner.ID),
		)

		owner = ctx.Ring.GetReplicaNode(parts[1])
	}

	if !ctx.Gossip.IsAlive(owner.ID) {

		logger.Log.Warn(
			"using replica due to node failure",
			zap.String("failed_node", owner.ID),
		)

		owner = ctx.Ring.GetReplicaNode(parts[1])
	}

	if !ctx.IsForwarded && owner.ID != ctx.NodeID {

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

	err := ctx.WAL.Write(command)

	if err != nil {

		logger.Log.Error(
			"wal write failed",
			zap.Error(err),
		)

		return "WAL write failed"
	}

	ctx.Store.Set(parts[1], parts[2])

	storedValue, _ := ctx.Store.GetValue(parts[1])

	// Owner has already stored the write.
	successfulWrites := 1

	for _, replica := range replicas {

		if replica.ID == ctx.NodeID {
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
			ctx.HandoffManager.AddHint(
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
