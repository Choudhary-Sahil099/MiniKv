package wal

import (
	"bufio"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"minikv/internal/storage"
)

func Recover(store *storage.Store, basePath string) error {
	dir := filepath.Dir(basePath)
	baseName := filepath.Base(basePath)

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var logFiles []string
	prefix := baseName + "."

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ".log") {
			logFiles = append(logFiles, filepath.Join(dir, name))
		}
	}

	sort.Strings(logFiles)

	for _, logPath := range logFiles {
		if err := replayFile(store, logPath); err != nil {
			return err
		}
	}

	return nil
}

func replayFile(store *storage.Store, path string) error {
	file, err := os.Open(path)
	if err != nil {
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