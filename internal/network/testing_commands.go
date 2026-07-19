package network

import (
	"strings"

	"minikv/internal/storage"
)

func HandleLocalGet(parts []string, store *storage.Store) string {

	if len(parts) != 2 {
		return "Usage: LOCAL_GET <key>"
	}

	key := strings.TrimSpace(parts[1])

	value, ok := store.GetValue(key)
	if !ok {
		return "Key not found"
	}

	return value.Data
}