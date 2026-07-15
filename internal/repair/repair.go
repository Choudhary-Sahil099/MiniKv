package repair

import (
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"minikv/internal/client"
	"minikv/internal/config"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/storage"
)

func StartAntiEntropy(
	store *storage.Store,
	replicaAddress string,
) {

	go func() {

		for {

			time.Sleep(
				config.AntiEntropyInterval,
			)

			data, err := client.RequestDump(
				replicaAddress,
			)

			if err != nil {
				continue
			}

			var replicaData map[string]storage.Value

			err = json.Unmarshal(
				data,
				&replicaData,
			)

			if err != nil {
				continue
			}

			localData := store.Export()

			repairs := 0

			for key, value := range localData {

				replicaValue,
					exists :=
					replicaData[key]

				if !exists {

					repairs++

					logger.Log.Info(
						"anti entropy repair",
						zap.String("key", key),
						zap.String("target", replicaAddress),
					)

					metrics.AntiEntropyRepairs.Inc()

					_, err := client.ForwardCommand(
						replicaAddress,
						"REPL_SET "+
							key+" "+
							value.Data+" "+
							value.CreatedAt.Format(time.RFC3339Nano),
					)

					if err != nil {
						continue
					}

					continue
				}

				if value.CreatedAt.After(replicaValue.CreatedAt) {

					repairs++

					logger.Log.Info(
						"anti entropy repair",
						zap.String("key", key),
						zap.String("target", replicaAddress),
					)

					metrics.AntiEntropyRepairs.Inc()

					_, err := client.ForwardCommand(
						replicaAddress,
						"REPL_SET "+
							key+" "+
							value.Data+" "+
							value.CreatedAt.Format(time.RFC3339Nano),
					)

					if err != nil {
						continue
					}
				} else if replicaValue.CreatedAt.After(value.CreatedAt) {

					repairs++

					logger.Log.Info(
						"anti entropy local repair",
						zap.String("key", key),
						zap.String("source", replicaAddress),
					)

					metrics.AntiEntropyRepairs.Inc()

					store.SetWithTimestamp(
						key,
						replicaValue.Data,
						replicaValue.CreatedAt,
					)
				}

			}

			if repairs > 0 {

				logger.Log.Info(
					"anti entropy completed",
					zap.Int("repairs", repairs),
				)
			}
		}
	}()
}
