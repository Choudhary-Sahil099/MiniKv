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

	return os.WriteFile(
		path,
		bytes,
		0644,
	)
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