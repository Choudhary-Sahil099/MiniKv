package network

import (
	"minikv/internal/hashring"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"strings"
)

func ProcessCommand(
	command string,
	nodeID string,
	store *storage.Store,
	wal *wal.WAL,
	ring *hashring.HashRing,
	isForwarded bool,
) string {

	parts := strings.Fields(command)

	if len(parts) == 0 {
		return "Empty command"
	}

	switch strings.ToUpper(parts[0]) {

	case "SET":

		if len(parts) != 3 {
			return "Usage: SET key value"
		}
		owner := ring.GetNode(parts[1])

		if !isForwarded && owner.ID != nodeID {

			response, err := ForwardCommand(
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

			go ForwardCommand(
				replica.Address,
				"REPL_SET "+parts[1]+" "+parts[2],
			)
		}
		return "OK"

	case "GET":

		if len(parts) != 2 {
			return "Usage: GET key"
		}

		owner := ring.GetNode(parts[1])

		if !isForwarded && owner.ID != nodeID {

			response, err := ForwardCommand(
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

		if len(parts) != 2 {
			return "Usage: DEL key"
		}

		owner := ring.GetNode(parts[1])

		if !isForwarded && owner.ID != nodeID {

			response, err := ForwardCommand(
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

	default:
		return "Unknown command"
	}
}
