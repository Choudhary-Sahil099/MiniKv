package snapshot

import (
	"encoding/json"
	"os"

	"minikv/internal/storage"
)

func Save(
	store *storage.Store,
	path string,
) error {

	data := store.Export()

	bytes, err := json.MarshalIndent(
		data,
		"",
		"  ",
	)

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

	return os.Rename(tmpPath, path)
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

	store.Import(dump)

	return nil
}
