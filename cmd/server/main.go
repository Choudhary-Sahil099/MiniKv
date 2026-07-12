package main

import (
	"go.uber.org/zap"
	"log"
	"minikv/internal/cluster"
	"minikv/internal/config"
	"minikv/internal/gossip"
	"minikv/internal/handoff"
	"minikv/internal/hashring"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/network"
	"minikv/internal/repair"
	"minikv/internal/snapshot"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"os"
	"time"
)

func main() {

	logger.Init()
	defer logger.Log.Sync()
	metrics.Init()

	go metrics.StartServer()
	if len(os.Args) != 3 {
		log.Fatal("Usage: go run ./cmd/server <NodeID> <Port>")
	}

	nodeID := os.Args[1]
	port := os.Args[2]
	address := "localhost:" + port

	store := storage.NewStore()
	handoffManager := handoff.NewManager()
	snapshotPath := "data/" + nodeID + ".snapshot"
	walPath := "data/" + nodeID + ".log"

	err := snapshot.Load(
		store,
		snapshotPath,
	)

	if err == nil {

		logger.Log.Info(
			"snapshot loaded",
			zap.String("file", snapshotPath),
		)

	} else {

		logger.Log.Info(
			"no snapshot found, starting fresh",
			zap.String("file", snapshotPath),
		)
	}

	err = wal.Recover(
		store,
		walPath,
	)

	if err != nil {
		log.Fatal(err)
	}

	walInstance, err := wal.NewWAL(walPath)
	if err != nil {
		log.Fatal(err)
	}

	ring := hashring.NewHashRing(config.VirtualNodes, config.ReplicationFactor)

	nodes := []cluster.Node{
		{
			ID:      "NodeA",
			Address: "localhost:5000",
		},
		{
			ID:      "NodeB",
			Address: "localhost:5001",
		},
		{
			ID:      "NodeC",
			Address: "localhost:5002",
		},
	}

	found := false

	for _, node := range nodes {

		if node.ID == nodeID {
			found = true
			break
		}
	}

	if !found {

		log.Fatalf(
			"nodeID %s not found in cluster configuration",
			nodeID,
		)
	}

	for _, node := range nodes {
		ring.AddNode(node)
	}
	g := gossip.NewGossip()
	g.SetNodeStatus(nodeID, true)
	g.StartHeartbeat(
		nodeID,
		nodes,
	)
	for _, node := range nodes {

		if node.ID == nodeID {
			continue
		}

		repair.StartAntiEntropy(
			store,
			node.Address,
		)
	} // Start Anti-Entropy with every replica

	go func() {

		for {

			time.Sleep(5 * time.Second)

			handoffManager.ReplayHints(
				g,
				nodes,
			)
		}

	}()
	go func() {

		for {

			time.Sleep(
				config.SnapshotInterval,
			)

			err := snapshot.Save(
				store,
				snapshotPath,
			)

			if err != nil {

				logger.Log.Error(
					"snapshot save failed",
					zap.Error(err),
				)

				continue
			}
			metrics.SnapshotsCreated.Inc()
			logger.Log.Info(
				"snapshot saved",
				zap.String("file", snapshotPath),
			)
			err = walInstance.Truncate()

			if err != nil {

				logger.Log.Error(
					"wal compaction failed",
					zap.Error(err),
				)

				continue
			}
			metrics.WALCompactions.Inc()
			logger.Log.Info(
				"wal compacted",
				zap.String("file", walPath),
			)
		}
	}()

	server := network.NewServer(
		address,
		nodeID,
		store,
		walInstance,
		ring,
		g,
		handoffManager,
	)

	err = server.Start()
	if err != nil {
		log.Fatal(err)
	}
}
