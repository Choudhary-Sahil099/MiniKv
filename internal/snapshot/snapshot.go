package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"minikv/internal/storage"
)

func Save(
	store *storage.Store,
	path string,
) error {

	data := store.Export()

	bytes, err := json.Marshal(data)

	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"

	file, err := os.OpenFile(
		tmpPath,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return err
	}
	// A failed write or sync must not leave behind a stale temporary snapshot.
	defer os.Remove(tmpPath)

	_, err = file.Write(bytes)
	if err != nil {
		file.Close()
		return err
	}

	err = file.Sync()
	if err != nil {
		file.Close()
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	// Sync the directory entry to make the rename durable on systems
	// supporting directory fsync (see sync_dir_* implementations).
	return syncSnapshotDirectory(filepath.Dir(path))
}

func Load(
	store *storage.Store,
	path string,
) error {

	bytes, err := os.ReadFile(path)

	if err != nil {
		return err
	}

	var data map[string]storage.Value

	err = json.Unmarshal(
		bytes,
		&data,
	)

	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("snapshot %q contains null instead of a key-value map", path)
	}

	store.Import(data)

	return nil
}
func ImportSnapshotBytes(
	store *storage.Store,
	data []byte,
) error {

	var dump map[string]storage.Value

	err := json.Unmarshal(
		data,
		&dump,
	)

	if err != nil {
		return err
	}
	if dump == nil {
		return fmt.Errorf("snapshot payload contains null instead of a key-value map")
	}

	store.Import(dump)

	return nil
}
