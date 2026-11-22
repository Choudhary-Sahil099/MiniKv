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

	"go.uber.org/zap"

	"minikv/internal/logger"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

type segmentFile struct {
	sequence int
	path     string
}

type recoveryStats struct {
	records                int
	setRecords             int
	versionedSetRecords    int
	deleteRecords          int
	versionedDeleteRecords int
	unknownRecords         int
	tailBytesTruncated     int64
}

func Recover(store *storage.Store, basePath string) error {
	startedAt := time.Now()

	dir := filepath.Dir(basePath)
	baseName := filepath.Base(basePath)

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Log.Info("WAL recovery skipped; directory does not exist",
				zap.String("directory", dir),
				zap.Duration("duration", time.Since(startedAt)),
			)
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

	logger.Log.Info("WAL recovery started",
		zap.String("wal_path", basePath),
		zap.Int("segments_discovered", len(segments)),
	)

	total := recoveryStats{}

	for i, segment := range segments {
		allowTailRepair := i == len(segments)-1
		segmentStartedAt := time.Now()

		stats, replayErr := replayFile(store, segment.path, allowTailRepair)
		if replayErr != nil {
			logger.Log.Error("WAL segment recovery failed",
				zap.String("segment", segment.path),
				zap.Duration("duration", time.Since(segmentStartedAt)),
				zap.Error(replayErr),
			)
			return fmt.Errorf("replay WAL segment %q: %w", segment.path, replayErr)
		}

		total.records += stats.records
		total.setRecords += stats.setRecords
		total.versionedSetRecords += stats.versionedSetRecords
		total.deleteRecords += stats.deleteRecords
		total.versionedDeleteRecords += stats.versionedDeleteRecords
		total.unknownRecords += stats.unknownRecords
		total.tailBytesTruncated += stats.tailBytesTruncated

		logger.Log.Info("WAL segment recovered",
			zap.String("segment", segment.path),
			zap.Int("records", stats.records),
			zap.Int64("tail_bytes_truncated", stats.tailBytesTruncated),
			zap.Duration("duration", time.Since(segmentStartedAt)),
		)
	}

	logger.Log.Info("WAL recovery completed",
		zap.String("wal_path", basePath),
		zap.Int("segments_recovered", len(segments)),
		zap.Int("records_replayed", total.records),
		zap.Int("set_records", total.setRecords),
		zap.Int("versioned_set_records", total.versionedSetRecords),
		zap.Int("delete_records", total.deleteRecords),
		zap.Int("versioned_delete_records", total.versionedDeleteRecords),
		zap.Int("unknown_records", total.unknownRecords),
		zap.Int64("tail_bytes_truncated", total.tailBytesTruncated),
		zap.Duration("duration", time.Since(startedAt)),
	)

	return nil
}

func replayFile(store *storage.Store, path string, allowTailRepair bool) (recoveryStats, error) {
	var stats recoveryStats

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return stats, err
	}
	defer file.Close()

	truncateAt, incomplete, err := incompleteTailOffset(file)
	if err != nil {
		return stats, fmt.Errorf("inspect incomplete WAL tail: %w", err)
	}

	if incomplete {
		if !allowTailRepair {
			return stats, fmt.Errorf("incomplete record found in a non-final WAL segment")
		}

		info, err := file.Stat()
		if err != nil {
			return stats, err
		}

		stats.tailBytesTruncated = info.Size() - truncateAt

		if err := file.Truncate(truncateAt); err != nil {
			return stats, fmt.Errorf("truncate incomplete WAL tail: %w", err)
		}
		if err := file.Sync(); err != nil {
			return stats, fmt.Errorf("sync truncated WAL tail: %w", err)
		}

		logger.Log.Warn("incomplete WAL tail truncated",
			zap.String("segment", path),
			zap.Int64("bytes_truncated", stats.tailBytesTruncated),
		)
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return stats, err
	}

	scanner := bufio.NewScanner(file)
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
			rest := strings.TrimSpace(line[len(parts[0]):])
			keyEnd := strings.IndexFunc(rest, func(r rune) bool {
				return r == ' ' || r == '\t'
			})
			if keyEnd <= 0 {
				return stats, fmt.Errorf("malformed SET record")
			}

			key := rest[:keyEnd]
			value := strings.TrimSpace(rest[keyEnd:])
			if value == "" {
				return stats, fmt.Errorf("malformed SET record for key %q: empty value", key)
			}

			store.Set(key, value)
			stats.records++
			stats.setRecords++

		case "DEL":
			if len(parts) != 2 {
				return stats, fmt.Errorf("malformed DEL record")
			}

			store.Delete(parts[1])
			stats.records++
			stats.deleteRecords++

		case "DEL_V2", "REPL_DEL":
			if len(parts) == 2 {
				store.Delete(parts[1])
				stats.deleteRecords++
				stats.versionedDeleteRecords++
				continue
			}

			if len(parts) != 4 {
				return stats, fmt.Errorf("malformed versioned delete record")
			}

			createdAt, parseErr := time.Parse(time.RFC3339Nano, parts[2])
			if parseErr != nil {
				return stats, fmt.Errorf("invalid delete timestamp: %w", parseErr)
			}

			clock, clockErr := vectorclock.Parse(parts[3])
			if clockErr != nil {
				return stats, fmt.Errorf("invalid delete vector clock: %w", clockErr)
			}

			store.MergeIfNewer(parts[1], storage.Value{
				CreatedAt: createdAt,
				Clock:     clock,
				Deleted:   true,
			})

			stats.records++
			stats.versionedDeleteRecords++

		case "SET_V2", "REPL_SET":
			if len(parts) < 5 {
				return stats, fmt.Errorf("malformed versioned SET record")
			}

			valueText, key, ok := parseVersionedSetPayload(line, parts)
			if !ok {
				return stats, fmt.Errorf("malformed versioned SET payload")
			}

			createdAt, parseErr := time.Parse(time.RFC3339Nano, parts[len(parts)-2])
			if parseErr != nil {
				return stats, fmt.Errorf("invalid SET timestamp: %w", parseErr)
			}

			clock, clockErr := vectorclock.Parse(parts[len(parts)-1])
			if clockErr != nil {
				return stats, fmt.Errorf("invalid SET vector clock: %w", clockErr)
			}

			store.MergeIfNewer(key, storage.Value{
				Data:      valueText,
				CreatedAt: createdAt,
				Clock:     clock,
			})

			stats.records++
			stats.versionedSetRecords++

		default:
			// Preserve existing behavior: unknown commands are ignored.
			stats.unknownRecords++
		}
	}

	if err := scanner.Err(); err != nil {
		return stats, err
	}

	return stats, nil
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
	firstSeparator := strings.IndexFunc(prefix, func(r rune) bool {
		return r == ' ' || r == '\t'
	})
	if firstSeparator < 0 {
		return "", "", false
	}

	rest := strings.TrimLeft(prefix[firstSeparator:], " \t")
	keySeparator := strings.IndexFunc(rest, func(r rune) bool {
		return r == ' ' || r == '\t'
	})
	if keySeparator <= 0 {
		return "", "", false
	}

	key = rest[:keySeparator]
	value = strings.TrimSpace(rest[keySeparator:])
	return value, key, value != ""
}

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
