package wal

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

type segmentFile struct {
	sequence int
	path     string
}

func Recover(store *storage.Store, basePath string) error {
	dir := filepath.Dir(basePath)
	baseName := filepath.Base(basePath)

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read WAL directory %q: %w", dir, err)
	}

	prefix := baseName + "."
	segments := make([]segmentFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".log") {
			continue
		}

		sequenceText := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".log")
		sequence, parseErr := strconv.Atoi(sequenceText)
		if parseErr != nil || sequence < 1 {
			continue
		}
		segments = append(segments, segmentFile{
			sequence: sequence,
			path:     filepath.Join(dir, name),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].sequence < segments[j].sequence
	})

	for i, segment := range segments {
		allowTailRepair := i == len(segments)-1
		if err := replayFile(store, segment.path, allowTailRepair); err != nil {
			return fmt.Errorf("replay WAL segment %q: %w", segment.path, err)
		}
	}
	return nil
}

func replayFile(store *storage.Store, path string, allowTailRepair bool) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	truncateAt, incomplete, err := incompleteTailOffset(file)
	if err != nil {
		return fmt.Errorf("inspect incomplete WAL tail: %w", err)
	}
	if incomplete {
		if !allowTailRepair {
			return fmt.Errorf("incomplete record found in a non-final WAL segment")
		}
		if err := file.Truncate(truncateAt); err != nil {
			return fmt.Errorf("truncate incomplete WAL tail: %w", err)
		}
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync truncated WAL tail: %w", err)
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	scanner := bufio.NewScanner(file)
	// WAL records can contain large values. Keep the parser bounded, but do
	// not use Scanner's small 64 KiB default token limit.
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch strings.ToUpper(parts[0]) {
		case "SET":
			// Preserve the complete value, including embedded spaces and tabs.
			rest := strings.TrimSpace(line[len(parts[0]):])
			keyEnd := strings.IndexFunc(rest, func(r rune) bool {
				return r == ' ' || r == '\t'
			})
			if keyEnd <= 0 {
				return fmt.Errorf("malformed SET record")
			}
			key := rest[:keyEnd]
			value := strings.TrimSpace(rest[keyEnd:])
			if value == "" {
				return fmt.Errorf("malformed SET record for key %q: empty value", key)
			}
			store.Set(key, value)

		case "DEL":
			if len(parts) != 2 {
				return fmt.Errorf("malformed DEL record")
			}
			store.Delete(parts[1])

		case "DEL_V2", "REPL_DEL":
			if len(parts) == 2 {
				// Legacy replica-delete record. New records carry a versioned tombstone.
				store.Delete(parts[1])
				continue
			}
			if len(parts) != 4 {
				return fmt.Errorf("malformed versioned delete record")
			}
			createdAt, parseErr := time.Parse(time.RFC3339Nano, parts[2])
			if parseErr != nil {
				return fmt.Errorf("invalid delete timestamp: %w", parseErr)
			}
			clock, clockErr := vectorclock.Parse(parts[3])
			if clockErr != nil {
				return fmt.Errorf("invalid delete vector clock: %w", clockErr)
			}
			store.MergeIfNewer(parts[1], storage.Value{
				CreatedAt: createdAt,
				Clock:     clock,
				Deleted:   true,
			})

		case "SET_V2", "REPL_SET":
			if len(parts) < 5 {
				return fmt.Errorf("malformed versioned SET record")
			}
			valueText, key, ok := parseVersionedSetPayload(line, parts)
			if !ok {
				return fmt.Errorf("malformed versioned SET payload")
			}
			createdAt, parseErr := time.Parse(time.RFC3339Nano, parts[len(parts)-2])
			if parseErr != nil {
				return fmt.Errorf("invalid SET timestamp: %w", parseErr)
			}
			clock, clockErr := vectorclock.Parse(parts[len(parts)-1])
			if clockErr != nil {
				return fmt.Errorf("invalid SET vector clock: %w", clockErr)
			}
			store.MergeIfNewer(key, storage.Value{
				Data:      valueText,
				CreatedAt: createdAt,
				Clock:     clock,
			})
		}
	}

	return scanner.Err()
}

func parseVersionedSetPayload(line string, fields []string) (value, key string, ok bool) {
	if len(fields) < 5 {
		return "", "", false
	}
	metadata := " " + fields[len(fields)-2] + " " + fields[len(fields)-1]
	metadataIndex := strings.LastIndex(line, metadata)
	if metadataIndex < 0 {
		return "", "", false
	}
	prefix := strings.TrimSpace(line[:metadataIndex])
	firstSeparator := strings.IndexFunc(prefix, func(r rune) bool { return r == ' ' || r == '\t' })
	if firstSeparator < 0 {
		return "", "", false
	}
	rest := strings.TrimLeft(prefix[firstSeparator:], " \t")
	keySeparator := strings.IndexFunc(rest, func(r rune) bool { return r == ' ' || r == '\t' })
	if keySeparator <= 0 {
		return "", "", false
	}
	key = rest[:keySeparator]
	value = strings.TrimSpace(rest[keySeparator:])
	return value, key, value != ""
}

// truncateIncompleteTail removes only bytes after the final complete newline-
// terminated record. WAL.Write always appends a newline, so a non-newline tail
// is an interrupted append and must never be replayed or appended behind.
func incompleteTailOffset(file *os.File) (int64, bool, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, false, err
	}
	size := info.Size()
	if size == 0 {
		return 0, false, nil
	}
	var last [1]byte
	if _, err := file.ReadAt(last[:], size-1); err != nil {
		return 0, false, err
	}
	if last[0] == '\n' {
		return size, false, nil
	}

	const chunkSize = 32 * 1024
	buffer := make([]byte, chunkSize)
	for end := size; end > 0; {
		start := end - chunkSize
		if start < 0 {
			start = 0
		}
		n := int(end - start)
		if _, err := file.ReadAt(buffer[:n], start); err != nil && err != io.EOF {
			return 0, false, err
		}
		if index := bytes.LastIndexByte(buffer[:n], '\n'); index >= 0 {
			return start + int64(index) + 1, true, nil
		}
		end = start
	}
	return 0, true, nil
}
