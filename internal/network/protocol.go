package network

import (
	"strings"

	"minikv/internal/storage"
)
import "minikv/internal/wal"

func ProcessCommand(
	command string,
	store *storage.Store,
	wal *wal.WAL,
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
		err := wal.Write(command)

		if err != nil {
			return "WAL write failed"
		}
		store.Set(parts[1], parts[2])

		return "OK"

	case "GET":

		if len(parts) != 2 {
			return "Usage: GET key"
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
		err := wal.Write(command)

		if err != nil {
			return "WAL write failed"
		}
		store.Delete(parts[1])

		return "Deleted"

	default:
		return "Unknown command"
	}
}
