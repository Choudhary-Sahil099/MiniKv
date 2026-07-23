package repair

import (
	"encoding/json"
	"minikv/internal/client"
	"minikv/internal/config"
	"minikv/internal/metrics"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
	"minikv/internal/merkle"
	"time"
)

func StartAntiEntropy(
	store *storage.Store,
	replicaAddress string,
) {
	go func() {
		for {
			time.Sleep(config.AntiEntropyInterval)

			remoteRoot, err := client.RequestMerkleRoot(replicaAddress)
			if err != nil {
				continue
			}

			localData := store.Export()

			localTree := merkle.Build(localData)
			localRoot := localTree.RootHash()

			if remoteRoot == localRoot {
				continue
			}

			data, err := client.RequestDump(replicaAddress)
			if err != nil {
				continue
			}

			var replicaData map[string]storage.Value
			err = json.Unmarshal(data, &replicaData)
			if err != nil {
				continue
			}

			for key, repVal := range replicaData {
				locVal, exists := localData[key]
				if !exists {
					metrics.AntiEntropyRepairs.Inc()
					store.SetValue(key, repVal)
					continue
				}

				relation := vectorclock.Compare(locVal.Clock, repVal.Clock)

				if relation == vectorclock.Before {
					metrics.AntiEntropyRepairs.Inc()
					store.SetValue(key, repVal)
				} else if relation == vectorclock.Concurrent {
					if repVal.CreatedAt.After(locVal.CreatedAt) {
						metrics.AntiEntropyRepairs.Inc()
						store.SetValue(key, repVal)
					}
				}
			}

			currentLocalData := store.Export()
			for key, locVal := range currentLocalData {
				repVal, exists := replicaData[key]
				shouldPush := false

				if !exists {
					shouldPush = true
				} else {
					relation := vectorclock.Compare(locVal.Clock, repVal.Clock)
					if relation == vectorclock.After {
						shouldPush = true
					} else if relation == vectorclock.Concurrent {
						if locVal.CreatedAt.After(repVal.CreatedAt) {
							shouldPush = true
						}
					}
				}

				if shouldPush {
					metrics.AntiEntropyRepairs.Inc()
					_, err := client.ForwardCommand(
						replicaAddress,
						"REPL_SET "+key+" "+locVal.Data+" "+locVal.CreatedAt.Format(time.RFC3339Nano)+" "+locVal.Clock.Serialize(),
					)
					if err != nil {
						continue
					}
				}
			}
		}
	}()
}
