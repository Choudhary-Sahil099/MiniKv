package network

import (
	"encoding/json"
	"go.uber.org/zap"
	"minikv/internal/client"
	"minikv/internal/gossip"
	"minikv/internal/hashring"
	"minikv/internal/logger"
	"minikv/internal/metrics"
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
) string {

	start := time.Now()

	defer func() {
		metrics.RequestLatency.Observe(
			time.Since(start).Seconds(),
		)
	}()

	metrics.TotalRequests.Inc()

	parts := strings.Fields(command)

	if len(parts) == 0 {
		return "Empty command"
	}

	switch strings.ToUpper(parts[0]) {

	case "SET":

		metrics.SetRequests.Inc()

		if len(parts) != 3 {
			return "Usage: SET key value"
		}

		owner := ring.GetNode(parts[1])
		replica := ring.GetReplicaNode(parts[1])
		logger.Log.Info(
			"placement",
			zap.String("key", parts[1]),
			zap.String("owner", owner.ID),
			zap.String("replica", replica.ID),
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

		if replica.ID != nodeID {

			metrics.ReplicationRequests.Inc()

			go func() {

				_, err := client.ForwardCommand(
					replica.Address,
					"REPL_SET "+parts[1]+" "+parts[2],
				)

				if err != nil {

					logger.Log.Error(
						"replication failed",
						zap.String("replica", replica.ID),
						zap.Error(err),
					)
				}
			}()
		}

		return "OK"

	case "GET":

		metrics.GetRequests.Inc()

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

		value, exists := store.Get(parts[1])

		if !exists {
			return "Key not found"
		}

		replica := ring.GetReplicaNode(parts[1])

		if replica.ID != nodeID {

			replicaValue, err := client.LocalGet(
				replica.Address,
				parts[1],
			)

			if err == nil {

				if replicaValue != value {
					metrics.ReadRepairs.Inc()
					logger.Log.Warn(
						"read repair triggered",
						zap.String("key", parts[1]),
						zap.String("replica", replica.ID),
					)

					go client.ForwardCommand(
						replica.Address,
						"REPL_SET "+parts[1]+" "+value,
					)
				}
			}
		}

		return value

	case "DEL":

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

		return "Deleted"

	case "REPL_SET":

		logger.Log.Info(
			"REPL_SET received",
			zap.String("node", nodeID),
			zap.String("key", parts[1]),
			zap.String("value", parts[2]),
		)

		if len(parts) != 3 {
			return "Usage: REPL_SET key value"
		}

		err := wal.Write(command)

		if err != nil {
			return "WAL write failed"
		}

		store.Set(parts[1], parts[2])

		return "REPLICATED"

	case "DUMP":

		data := store.Export()

		bytes, err := json.Marshal(data)

		if err != nil {

			logger.Log.Error(
				"dump marshal failed",
				zap.Error(err),
			)

			return "DUMP_FAILED"
		}

		return string(bytes) + "\n"

	case "LOCAL_GET":

		if len(parts) != 2 {
			return "Usage: LOCAL_GET key"
		}

		value, exists := store.Get(parts[1])

		if !exists {
			return "NOT_FOUND"
		}

		return value

	case "PING":

		return "PONG"

	default:

		logger.Log.Warn(
			"unknown command received",
			zap.String("command", command),
		)

		return "Unknown command"
	}
}
