package repair

import (
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"minikv/internal/client"
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
				30 * time.Second,
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

				if !exists ||
					replicaValue.Data != value.Data {
					repairs++
					logger.Log.Info(
						"anti entropy repair",
						zap.String("key", key),
						zap.String("target", replicaAddress),
					)
					metrics.AntiEntropyRepairs.Inc()
					_, err := client.ForwardCommand(
						replicaAddress,
						"REPL_SET "+key+" "+value.Data,
					)

					if err != nil {
						continue
					}
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
