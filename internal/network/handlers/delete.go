package handlers

import (
	"go.uber.org/zap"
	"minikv/internal/client"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/network/common"
	"strings"
)

func HandleDEL(
	ctx *common.CommandContext,
	command string,
) string {

	parts := strings.Fields(command)

	if len(parts) != 2 {
		return "Usage: DEL key"
	}

	metrics.DelRequests.Inc()

	if len(parts) != 2 {
		return "Usage: DEL key"
	}

	owner := ctx.Ring.GetNode(parts[1])

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

	ctx.Store.Delete(parts[1])
	replicas := ctx.Ring.GetReplicaNodes(parts[1])

	for _, replica := range replicas {

		if replica.ID == ctx.NodeID {
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
