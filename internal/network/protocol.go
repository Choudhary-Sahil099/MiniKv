package network

import (
	"fmt"
	"minikv/internal/hashring"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"strings"
	"minikv/internal/gossip"
	"minikv/internal/client"
	"minikv/internal/metrics"

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
		if !g.IsAlive(owner.ID) {

			fmt.Println(owner.ID, "is dead, using replica")

			owner = ring.GetReplicaNode(parts[1])
		}

		if !isForwarded && owner.ID != nodeID {

			response, err := client.ForwardCommand(
				owner.Address,
				command,
			)

			if err != nil {
				return "Forwarding failed"
			}

			return response
		}
		err := wal.Write(command)

		if err != nil {
			return "WAL write failed"
		}
		store.Set(parts[1], parts[2])
		replica := ring.GetReplicaNode(parts[1])

		if replica.ID != nodeID {

			go client.ForwardCommand(
				replica.Address,
				"REPL_SET "+parts[1]+" "+parts[2],
			)
		}
		return "OK"

	case "GET":
		metrics.GetRequests.Inc()
		if len(parts) != 2 {
			return "Usage: GET key"
		}

		owner := ring.GetNode(parts[1])
		if !g.IsAlive(owner.ID) {

			fmt.Println(owner.ID, "is dead, using replica")

			owner = ring.GetReplicaNode(parts[1])
		}

		if !isForwarded && owner.ID != nodeID {

			response, err := client.ForwardCommand(
				owner.Address,
				command,
			)

			if err != nil {
				return "Forwarding failed"
			}

			return response
		}

		value, exists := store.Get(parts[1])

		if !exists {
			return "Key not found"
		}

		return value

	case "DEL":
		metrics.DelRequests.Inc()
		if len(parts) != 2 {
			return "Usage: DEL key"
		}

		owner := ring.GetNode(parts[1])
		if !g.IsAlive(owner.ID) {

			fmt.Println(owner.ID, "is dead, using replica")

			owner = ring.GetReplicaNode(parts[1])
		}

		if !isForwarded && owner.ID != nodeID {

			response, err := client.ForwardCommand(
				owner.Address,
				command,
			)

			if err != nil {
				return "Forwarding failed"
			}

			return response
		}

		err := wal.Write(command)

		if err != nil {
			return "WAL write failed"
		}

		store.Delete(parts[1])

		return "Deleted"

	case "REPL_SET":

		if len(parts) != 3 {
			return "Usage: REPL_SET key value"
		}

		err := wal.Write(command)

		if err != nil {
			return "WAL write failed"
		}

		store.Set(parts[1], parts[2])

		return "REPLICATED"

	// this is the clusters internal communication
	case "PING":
		return "PONG"
	default:
		return "Unknown command"
	}
}
