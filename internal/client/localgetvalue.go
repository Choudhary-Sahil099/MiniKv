package client

import (
	"encoding/json"

	"minikv/internal/storage"
)

func LocalGetValue(
	address string,
	key string,
) (storage.Value, error) {

	response, err := ForwardCommand(
		address,
		"LOCAL_GET_VALUE "+key,
	)

	if err != nil {
		return storage.Value{}, err
	}
	if response == "NOT_FOUND" {
		// A missing key is still a successful replica read response.
		return storage.Value{}, nil
	}

	var value storage.Value

	err = json.Unmarshal(
		[]byte(response),
		&value,
	)

	if err != nil {
		return storage.Value{}, err
	}

	return value, nil
}
