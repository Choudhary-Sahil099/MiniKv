package handlers

import (
	"strings"

	"minikv/internal/storage"
)

func HandleLocalGet(
	command string,
	store *storage.Store,
) string {

	parts := strings.Fields(command)

	if len(parts) != 2 {
		return "Usage: LOCAL_GET key"
	}

	value, exists := store.Get(parts[1])

	if !exists {
		return "NOT_FOUND"
	}

	return value
}