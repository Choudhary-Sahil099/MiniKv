package handlers

import (
	"go.uber.org/zap"
	"minikv/internal/client"
	"minikv/internal/config"
	"minikv/internal/handoff"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/network/common"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
	"minikv/internal/cluster"
	"strings"
	"time"
)

func HandleSET(
	ctx *common.CommandContext,
	command string,
) string {
	parts := strings.SplitN(strings.TrimSpace(command), " ", 3)

	if len(parts) < 3 {
		return "Usage: SET key value"
	}

	metrics.SetRequests.
		WithLabelValues(ctx.NodeID).
		Inc()

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

	var clock vectorclock.VectorClock
	existingValue, exists := ctx.Store.GetValue(parts[1])
	if exists {
		clock = existingValue.Clock.Copy()
	} else {
		clock = make(vectorclock.VectorClock)
	}

	clock.Increment(ctx.NodeID)

	value := storage.Value{
		Data:      parts[2],
		CreatedAt: time.Now(),
		Clock:     clock,
	}

	ctx.Store.SetValue(parts[1], value)
	storedValue := value

	successfulWrites := 1

	type replicationResult struct {
		replica  cluster.Node
		command  string
		response string
		err      error
	}

	results := make(chan replicationResult, len(replicas))
	pending := 0

	for _, replica := range replicas {
		if replica.ID == ctx.NodeID {
			continue
		}

		metrics.ReplicationRequests.Inc()

		replicationCommand :=
			"REPL_SET " +
				parts[1] + " " +
				parts[2] + " " +
				storedValue.CreatedAt.Format(time.RFC3339Nano) + " " +
				storedValue.Clock.Serialize()

		pending++

		go func(replica cluster.Node, command string) {
			response, err := client.ForwardCommand(
				replica.Address,
				command,
			)

			results <- replicationResult{
				replica:  replica,
				command:  command,
				response: response,
				err:      err,
			}
		}(replica, replicationCommand)
	}

	handleResult := func(result replicationResult) {
		if result.err != nil {
			logger.Log.Warn(
				"replication failed",
				zap.String("replica", result.replica.ID),
				zap.Error(result.err),
			)

			ctx.HandoffManager.AddHint(handoff.Hint{
				TargetNode: result.replica.ID,
				Command:    result.command,
				CreatedAt:  time.Now(),
			})
			return
		}

		if result.response == "REPLICATED" {
			successfulWrites++
			return
		}

		logger.Log.Warn(
			"replica did not acknowledge write",
			zap.String("key", parts[1]),
			zap.String("replica", result.replica.ID),
			zap.String("response", result.response),
			zap.Int("acks", successfulWrites),
		)
	}

	for i := 0; i < pending; i++ {
		handleResult(<-results)
		if successfulWrites >= config.WriteQuorum {
			if remaining := pending - i - 1; remaining > 0 {
				go func() {
					for j := 0; j < remaining; j++ {
						handleResult(<-results)
					}
				}()
			}
			return "OK"
		}
	}

	logger.Log.Error(
		"write quorum failed",
		zap.Int("acks", successfulWrites),
	)

	return "WRITE_QUORUM_FAILED"
}
