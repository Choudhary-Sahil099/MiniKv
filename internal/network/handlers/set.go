package handlers

import (
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
	"strings"
	"time"
	"unicode"
)

func HandleSET(
	ctx *common.CommandContext,
	command string,
) string {
	trimmed := strings.TrimSpace(command)
	parts := strings.Fields(trimmed)
	if len(parts) < 3 {
		return "Usage: SET key value"
	}

	firstSeparator := strings.IndexFunc(trimmed, unicode.IsSpace)
	remaining := strings.TrimLeftFunc(trimmed[firstSeparator:], unicode.IsSpace)
	keySeparator := strings.IndexFunc(remaining, unicode.IsSpace)
	if firstSeparator < 0 || keySeparator <= 0 {
		return "Usage: SET key value"
	}
	key := remaining[:keySeparator]
	valueData := strings.TrimSpace(remaining[keySeparator:])
	if valueData == "" {
		return "Usage: SET key value"
	}

	metrics.SetRequests.
		WithLabelValues(ctx.NodeID).
		Inc()

	owner := ctx.Ring.GetNode(key)
	replicas := ctx.Ring.GetReplicaNodes(key)

	replicaIDs := []string{}
	for _, replica := range replicas {
		replicaIDs = append(replicaIDs, replica.ID)
	}

	logger.Log.Debug(
		"write placement",
		zap.String("owner", owner.ID),
		zap.Strings("replicas", replicaIDs),
	)

	if !ctx.Gossip.IsAlive(owner.ID) {
		logger.Log.Warn(
			"using replica due to node failure",
			zap.String("failed_node", owner.ID),
		)
		owner = ctx.Ring.GetReplicaNode(key)
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

	var value storage.Value
	writeErr := func() error {
		unlock := ctx.Store.LockKey(key)
		defer unlock()

		clock := make(vectorclock.VectorClock)
		if existingValue, exists := ctx.Store.GetValue(key); exists {
			clock = existingValue.Clock.Copy()
		}
		clock.Increment(ctx.NodeID)

		value = storage.Value{
			Data:      valueData,
			CreatedAt: time.Now(),
			Clock:     clock,
		}

		// Persist the full value and causal metadata before publishing it in memory.
		walRecord := "SET_V2 " + key + " " + value.Data + " " +
			value.CreatedAt.Format(time.RFC3339Nano) + " " + value.Clock.Serialize()
		if err := ctx.WAL.Write(walRecord); err != nil {
			return err
		}
		ctx.Store.SetValue(key, value)
		return nil
	}()
	if writeErr != nil {
		logger.Log.Error("wal write failed", zap.Error(writeErr))
		return "WAL write failed"
	}
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
				key + " " +
				valueData + " " +
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

		if result.response == "REPLICATED" || result.response == "DUPLICATE_VERSION" {
			successfulWrites++
			return
		}

		logger.Log.Warn(
			"replica did not acknowledge write",
			zap.String("key", key),
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
