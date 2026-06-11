package snapshot

import (
	"minikv/internal/client"
	"minikv/internal/storage"
)

func SyncFromNode(
	store *storage.Store,
	address string,
) error {

	data, err := client.RequestDump(
		address,
	)

	if err != nil {
		return err
	}

	return ImportSnapshotBytes(
		store,
		data,
	)
}