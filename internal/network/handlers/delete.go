package handlers

import (
	"strings"
	"time"

	"go.uber.org/zap"

	"minikv/internal/client"
	"minikv/internal/cluster"
	"minikv/internal/config"
	"minikv/internal/handoff"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/network/common"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

func HandleDEL(ctx *common.CommandContext, command string) string {
	parts := strings.Fields(command)
	if len(parts) != 2 {
		return "Usage: DEL key"
	}
	key := parts[1]
	metrics.DelRequests.Inc()

	owner := ctx.Ring.GetNode(key)
	if !ctx.Gossip.IsAlive(owner.ID) {
		logger.Log.Warn("using replica due to node failure", zap.String("failed_node", owner.ID))
		owner = ctx.Ring.GetReplicaNode(key)
	}

	if !ctx.IsForwarded && owner.ID != ctx.NodeID {
		metrics.ForwardedRequests.Inc()
		response, err := client.ForwardCommand(owner.Address, command)
		if err != nil {
			logger.Log.Error("delete forwarding failed", zap.String("target", owner.ID), zap.Error(err))
			return "Forwarding failed"
		}
		return response
	}

	var tombstone storage.Value
	writeErr := func() error {
		unlock := ctx.Store.LockKey(key)
		defer unlock()

		clock := make(vectorclock.VectorClock)
		if existing, exists := ctx.Store.GetValue(key); exists {
			clock = existing.Clock.Copy()
		}
		clock.Increment(ctx.NodeID)

		tombstone = storage.Value{
			CreatedAt: time.Now(),
			Clock:     clock,
			Deleted:   true,
		}
		record := "DEL_V2 " + key + " " + tombstone.CreatedAt.Format(time.RFC3339Nano) + " " + tombstone.Clock.Serialize()
		if err := ctx.WAL.Write(record); err != nil {
			return err
		}
		ctx.Store.SetTombstone(key, tombstone)
		return nil
	}()
	if writeErr != nil {
		logger.Log.Error("delete WAL write failed", zap.Error(writeErr))
		return "WAL write failed"
	}

	type replicationResult struct {
		replica  cluster.Node
		command  string
		response string
		err      error
	}

	replicas := ctx.Ring.GetReplicaNodes(key)
	results := make(chan replicationResult, len(replicas))
	pending := 0
	for _, replica := range replicas {
		if replica.ID == ctx.NodeID {
			continue
		}
		metrics.ReplicationRequests.Inc()
		replicationCommand := "REPL_DEL " + key + " " + tombstone.CreatedAt.Format(time.RFC3339Nano) + " " + tombstone.Clock.Serialize()
		pending++
		go func(replica cluster.Node, replicationCommand string) {
			response, err := client.ForwardCommand(replica.Address, replicationCommand)
			results <- replicationResult{replica: replica, command: replicationCommand, response: response, err: err}
		}(replica, replicationCommand)
	}

	successfulWrites := 1 // the local tombstone is WAL-synced before this point
	handleResult := func(result replicationResult) {
		if result.err != nil {
			logger.Log.Warn("delete replication failed", zap.String("replica", result.replica.ID), zap.Error(result.err))
			if ctx.HandoffManager != nil {
				ctx.HandoffManager.AddHint(handoff.Hint{TargetNode: result.replica.ID, Command: result.command, CreatedAt: time.Now()})
			}
			return
		}
		if result.response == "DELETED" || result.response == "DUPLICATE_VERSION" {
			successfulWrites++
			return
		}
		logger.Log.Warn("replica did not acknowledge delete", zap.String("replica", result.replica.ID), zap.String("response", result.response))
	}

	for i := 0; i < pending; i++ {
		handleResult(<-results)
		if successfulWrites >= config.WriteQuorum {
			if remaining := pending - i - 1; remaining > 0 {
				go func(count int) {
					for j := 0; j < count; j++ {
						handleResult(<-results)
					}
				}(remaining)
			}
			return "Deleted"
		}
	}

	logger.Log.Error("delete write quorum failed", zap.Int("acks", successfulWrites), zap.Int("required", config.WriteQuorum))
	return "WRITE_QUORUM_FAILED"
}
