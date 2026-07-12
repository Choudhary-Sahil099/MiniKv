package handlers

import (
	"strings"

	"minikv/internal/storage"
	"minikv/internal/wal"
)

func HandleReplDelete(
	command string,
	store *storage.Store,
	wal *wal.WAL,
) string {

	parts := strings.Fields(command)

	if len(parts) != 2 {
		return "Usage: REPL_DEL key"
	}

	err := wal.Write(command)
	if err != nil {
		return "WAL write failed"
	}

	store.Delete(parts[1])

	return "DELETED"
}
