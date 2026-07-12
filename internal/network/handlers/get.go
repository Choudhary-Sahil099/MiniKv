package handlers

import (
	"strings"
	"time"
	"go.uber.org/zap"

	"minikv/internal/client"
	"minikv/internal/cluster"
	"minikv/internal/config"
	"minikv/internal/gossip"
	"minikv/internal/hashring"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/storage"
	"minikv/internal/wal"
)
func HandleGET(
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
		return "Usage: GET key"
	}
	metrics.GetRequests.
			WithLabelValues(nodeID).
			Inc()

		if len(parts) != 2 {
			return "Usage: GET key"
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
		ownerValue, exists := store.GetValue(parts[1])

		if !exists {
			return "Key not found"
		}
		value := ownerValue.Data
		storedValue := ownerValue
		versions := []storage.Value{
			ownerValue,
		}
		replicaVersions := make(map[string]storage.Value)
		successfulReads := 1
		replicas := ring.GetReplicaNodes(parts[1])
		for _, replica := range replicas {

			if replica.ID == nodeID {
				continue
			}

			replicaValue, err := client.LocalGetValue(
				replica.Address,
				parts[1],
			)

			if err != nil {
				continue
			}

			successfulReads++

			versions = append(
				versions,
				replicaValue,
			)

			replicaVersions[replica.ID] = replicaValue
		}
		if successfulReads < config.ReadQuorum {

			logger.Log.Error(
				"read quorum failed",
				zap.Int("responses", successfulReads),
			)

			return "READ_QUORUM_FAILED"
		}
		logger.Log.Info(
			"read quorum achieved",
			zap.Int("responses", successfulReads),
		)
		latest := versions[0]

		for _, version := range versions {

			if version.CreatedAt.After(latest.CreatedAt) {
				latest = version
			}
		}

		value = latest.Data
		storedValue = latest

		for _, replica := range replicas {

			if replica.ID == nodeID {
				continue
			}

			replicaValue, exists := replicaVersions[replica.ID]

			if !exists {
				continue
			}

			if !replicaValue.CreatedAt.Before(storedValue.CreatedAt) {
				continue
			}

			metrics.ReadRepairs.Inc()

			logger.Log.Warn(
				"read repair triggered",
				zap.String("key", parts[1]),
				zap.String("replica", replica.ID),
			)

			go func(replica cluster.Node) {

				_, err := client.ForwardCommand(
					replica.Address,
					"REPL_SET "+parts[1]+" "+value+" "+storedValue.CreatedAt.Format(time.RFC3339Nano),
				)

				if err != nil {

					logger.Log.Error(
						"read repair failed",
						zap.String("replica", replica.ID),
						zap.Error(err),
					)
				}

			}(replica)
		}

		return value

}