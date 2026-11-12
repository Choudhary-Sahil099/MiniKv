package repair

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"minikv/internal/client"
	"minikv/internal/config"
	"minikv/internal/logger"
	"minikv/internal/merkle"
	"minikv/internal/metrics"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
	"minikv/internal/wal"
)

func StartAntiEntropy(ctx context.Context, store *storage.Store, log *wal.WAL, replicaAddress string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(config.AntiEntropyInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if ctx.Err() != nil {
				return
			}

			remoteRoot, err := client.RequestMerkleRoot(replicaAddress)
			if err != nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}

			localData := store.Export()
			localRoot := merkle.Build(localData).RootHash()
			if remoteRoot == localRoot {
				continue
			}

			data, err := client.RequestDump(replicaAddress)
			if err != nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}

			var replicaData map[string]storage.Value
			if err := json.Unmarshal(data, &replicaData); err != nil {
				logger.Log.Warn("anti-entropy dump decode failed", zap.String("peer", replicaAddress), zap.Error(err))
				continue
			}

			for key, remoteValue := range replicaData {
				if applyLocalRepair(store, log, key, remoteValue) {
					localData[key] = remoteValue
					metrics.AntiEntropyRepairs.Inc()
				}
			}

			for key, localValue := range localData {
				remoteValue, exists := replicaData[key]
				if exists && !versionIsNewer(localValue, remoteValue) {
					continue
				}

				command := "REPL_SET " + key + " " + localValue.Data + " " + localValue.CreatedAt.Format(time.RFC3339Nano) + " " + localValue.Clock.Serialize()
				if localValue.Deleted {
					command = "REPL_DEL " + key + " " + localValue.CreatedAt.Format(time.RFC3339Nano) + " " + localValue.Clock.Serialize()
				}
				if _, err := client.ForwardCommand(replicaAddress, command); err != nil {
					logger.Log.Warn("anti-entropy push failed", zap.String("peer", replicaAddress), zap.String("key", key), zap.Error(err))
					continue
				}
				metrics.AntiEntropyRepairs.Inc()
			}
		}
	}()
	return done
}

func applyLocalRepair(store *storage.Store, log *wal.WAL, key string, candidate storage.Value) bool {
	unlock := store.LockKey(key)
	defer unlock()

	current, exists := store.GetValue(key)
	if exists && !versionIsNewer(candidate, current) {
		return false
	}

	var record string
	if candidate.Deleted {
		record = "DEL_V2 " + key + " " + candidate.CreatedAt.Format(time.RFC3339Nano) + " " + candidate.Clock.Serialize()
	} else {
		record = "SET_V2 " + key + " " + candidate.Data + " " + candidate.CreatedAt.Format(time.RFC3339Nano) + " " + candidate.Clock.Serialize()
	}
	if err := log.Write(record); err != nil {
		logger.Log.Error("anti-entropy WAL write failed", zap.String("key", key), zap.Error(err))
		return false
	}
	return store.MergeIfNewer(key, candidate)
}

func versionIsNewer(candidate, current storage.Value) bool {
	relation := vectorclock.Compare(candidate.Clock, current.Clock)
	switch relation {
	case vectorclock.After:
		return true
	case vectorclock.Before:
		return false
	case vectorclock.Concurrent, vectorclock.Equal:
		return candidate.CreatedAt.After(current.CreatedAt)
	default:
		return false
	}
}
