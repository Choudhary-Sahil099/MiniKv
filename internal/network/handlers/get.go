package handlers

import (
	"strings"
	"time"

	"go.uber.org/zap"

	"minikv/internal/client"
	"minikv/internal/cluster"
	"minikv/internal/config"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/network/common"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

type replicaReadResult struct {
	replica cluster.Node
	value   storage.Value
	err     error
}

func HandleGET(ctx *common.CommandContext, command string) string {
	parts := strings.Fields(command)
	if len(parts) != 2 {
		return "Usage: GET key"
	}
	key := parts[1]
	metrics.GetRequests.WithLabelValues(ctx.NodeID).Inc()

	owner := ctx.Ring.GetNode(key)
	if !ctx.Gossip.IsAlive(owner.ID) {
		logger.Log.Warn("using replica due to node failure", zap.String("failed_node", owner.ID))
		owner = ctx.Ring.GetReplicaNode(key)
	}
	if !ctx.IsForwarded && owner.ID != ctx.NodeID {
		metrics.ForwardedRequests.Inc()
		response, err := client.ForwardReadCommand(owner.Address, command)
		if err != nil {
			logger.Log.Error("read forwarding failed", zap.String("target", owner.ID), zap.Error(err))
			return "Forwarding failed"
		}
		return response
	}

	type version struct {
		node  string
		value storage.Value
	}

	localValue, localExists := ctx.Store.GetValue(key)
	versions := make([]version, 0, config.ReplicationFactor)
	if localExists {
		versions = append(versions, version{node: ctx.NodeID, value: localValue})
	}

	replicas := ctx.Ring.GetReplicaNodes(key)
	results := make(chan replicaReadResult, len(replicas))
	pending := 0
	for _, replica := range replicas {
		if replica.ID == ctx.NodeID {
			continue
		}
		pending++
		go func(replica cluster.Node) {
			value, err := client.LocalGetValue(replica.Address, key)
			results <- replicaReadResult{replica: replica, value: value, err: err}
		}(replica)
	}

	// The local store always supplies one successful response, including a
	// legitimate absence. Remote NOT_FOUND responses also count as responses.
	successfulReads := 1
	remoteValues := make(map[string]storage.Value, pending)
	remoteResponded := make(map[string]bool, pending)
	// A quorum read needs ReadQuorum successful responses, not every replica.
	// Each remote call has a bounded client deadline and writes into a channel
	// buffered for all pending replicas, so slower responses cannot block their
	// goroutines after this request has returned. Periodic anti-entropy repairs
	// replicas that were not needed to satisfy this read quorum.
	for i := 0; i < pending && successfulReads < config.ReadQuorum; i++ {
		result := <-results
		if result.err != nil {
			logger.Log.Warn("replica read failed", zap.String("replica", result.replica.ID), zap.Error(result.err))
			continue
		}
		successfulReads++
		remoteResponded[result.replica.ID] = true
		remoteValues[result.replica.ID] = result.value
		if hasVersion(result.value) {
			versions = append(versions, version{node: result.replica.ID, value: result.value})
		}
	}

	if successfulReads < config.ReadQuorum {
		logger.Log.Error("read quorum failed", zap.Int("responses", successfulReads), zap.Int("required", config.ReadQuorum))
		return "READ_QUORUM_FAILED"
	}
	if len(versions) == 0 {
		return "Key not found"
	}

	latest := versions[0].value
	for _, candidate := range versions[1:] {
		if versionIsNewer(candidate.value, latest) {
			latest = candidate.value
		}
	}

	// Repair the local copy if a replica supplied a causally newer version.
	if !localExists || versionIsNewer(latest, localValue) {
		if err := persistRepair(ctx, key, latest); err != nil {
			logger.Log.Error("local read repair persistence failed", zap.String("key", key), zap.Error(err))
		} else {
			metrics.ReadRepairs.Inc()
		}
	}

	for _, replica := range replicas {
		if replica.ID == ctx.NodeID {
			continue
		}
		if !remoteResponded[replica.ID] {
			continue
		}
		current := remoteValues[replica.ID]
		if hasVersion(current) && !versionIsNewer(latest, current) {
			continue
		}
		if hasVersion(current) && current.Deleted == latest.Deleted && current.Data == latest.Data &&
			current.CreatedAt.Equal(latest.CreatedAt) && vectorclock.Compare(current.Clock, latest.Clock) == vectorclock.Equal {
			continue
		}

		go func(replica cluster.Node, repaired storage.Value) {
			var repairCommand string
			if repaired.Deleted {
				repairCommand = "REPL_DEL " + key + " " + repaired.CreatedAt.Format(time.RFC3339Nano) + " " + repaired.Clock.Serialize()
			} else {
				repairCommand = "REPL_SET " + key + " " + repaired.Data + " " + repaired.CreatedAt.Format(time.RFC3339Nano) + " " + repaired.Clock.Serialize()
			}
			response, err := client.ForwardCommand(replica.Address, repairCommand)
			if err != nil {
				logger.Log.Error("read repair failed", zap.String("replica", replica.ID), zap.Error(err))
				return
			}
			if response == "REPLICATED" || response == "DELETED" || response == "DUPLICATE_VERSION" {
				metrics.ReadRepairs.Inc()
			}
			logger.Log.Debug("read repair completed", zap.String("replica", replica.ID), zap.String("response", response))
		}(replica, latest)
	}

	if latest.Deleted {
		return "Key not found"
	}
	return latest.Data
}

func hasVersion(value storage.Value) bool {
	return !value.CreatedAt.IsZero() || value.Data != "" || value.Deleted || len(value.Clock) > 0
}

func versionIsNewer(candidate, current storage.Value) bool {
	relation := vectorclock.Compare(candidate.Clock, current.Clock)
	switch relation {
	case vectorclock.After:
		return true
	case vectorclock.Before:
		return false
	case vectorclock.Concurrent, vectorclock.Equal:
		return candidate.CreatedAt.After(current.CreatedAt)
	default:
		return false
	}
}

func persistRepair(ctx *common.CommandContext, key string, value storage.Value) error {
	unlock := ctx.Store.LockKey(key)
	defer unlock()

	current, exists := ctx.Store.GetValue(key)
	if exists && !versionIsNewer(value, current) {
		return nil
	}

	var record string
	if value.Deleted {
		record = "DEL_V2 " + key + " " + value.CreatedAt.Format(time.RFC3339Nano) + " " + value.Clock.Serialize()
	} else {
		record = "SET_V2 " + key + " " + value.Data + " " + value.CreatedAt.Format(time.RFC3339Nano) + " " + value.Clock.Serialize()
	}
	if err := ctx.WAL.Write(record); err != nil {
		return err
	}
	ctx.Store.MergeIfNewer(key, value)
	return nil
}
