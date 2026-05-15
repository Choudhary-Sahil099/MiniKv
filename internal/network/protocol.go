package network

import (
	"strings"

	"minikv/internal/storage"
)

func ProcessCommand(command string, store *storage.Store) string {

	parts := strings.Fields(command)

	if len(parts) == 0 {
		return "Empty command"
	}

	switch strings.ToUpper(parts[0]) {

	case "SET":

		if len(parts) != 3 {
			return "Usage: SET key value"
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

		store.Delete(parts[1])

		return "Deleted"

	default:
		return "Unknown command"
	}
}