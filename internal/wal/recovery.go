package wal

import (
	"bufio"
	"os"
	"strings"

	"minikv/internal/storage"
)

func Recover(store *storage.Store, path string) error {

	file, err := os.Open(path)

	if err != nil {

		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {

		line := scanner.Text()

		parts := strings.Fields(line)

		if len(parts) == 0 {
			continue
		}

		switch strings.ToUpper(parts[0]) {

		case "SET":

			if len(parts) != 3 {
				continue
			}

			store.Set(parts[1], parts[2])

		case "DEL":

			if len(parts) != 2 {
				continue
			}

			store.Delete(parts[1])
		}
	}

	return scanner.Err()
}
