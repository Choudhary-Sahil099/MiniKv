package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"

	clusterconfig "minikv/config"
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
)

func main() {
	logger.Init()
	metrics.Init()

	if err := run(); err != nil {
		logger.Log.Error("MiniKV server stopped with an error", zap.Error(err))
		_ = logger.Log.Sync()
		os.Exit(1)
	}
	_ = logger.Log.Sync()
}

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: minikv-server <NodeID> <Port>")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	nodeID := os.Args[1]
	port := os.Args[2]
	bindHost := envOrDefault("MINIKV_BIND_HOST", "127.0.0.1")
	address := net.JoinHostPort(bindHost, port)
	clusterConfigPath := envOrDefault("MINIKV_CLUSTER_CONFIG", "config/cluster.json")
	metricsAddress := envOrDefault("MINIKV_METRICS_ADDR", "127.0.0.1:2112")
	dataDir := envOrDefault("MINIKV_DATA_DIR", "data")
	maxConnections, err := intEnvOrDefault("MINIKV_MAX_CONNECTIONS", networkDefaultMaxConnections)
	if err != nil {
		return err
	}
	walSegmentBytes, err := int64EnvOrDefault("MINIKV_WAL_SEGMENT_BYTES", wal.DefaultSegmentBytes)
	if err != nil {
		return err
	}

	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		if err := metrics.StartServer(ctx, metricsAddress); err != nil && ctx.Err() == nil {
			logger.Log.Error("metrics server stopped", zap.Error(err))
		}
	}()

	store := storage.NewStore()
	handoffManager := handoff.NewManager()
	snapshotPath := filepath.Join(dataDir, nodeID+".snapshot")
	walPath := filepath.Join(dataDir, nodeID)

	if err := snapshot.Load(store, snapshotPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Log.Info("no snapshot found, starting fresh", zap.String("file", snapshotPath))
		} else {
			return fmt.Errorf("load snapshot %q: %w", snapshotPath, err)
		}
	} else {
		logger.Log.Info("snapshot loaded", zap.String("file", snapshotPath))
	}

	if err := wal.Recover(store, walPath); err != nil {
		return fmt.Errorf("recover WAL %q: %w", walPath, err)
	}

	walInstance, err := wal.NewWALWithSegmentSize(walPath, walSegmentBytes)
	if err != nil {
		return fmt.Errorf("open WAL %q: %w", walPath, err)
	}
	defer walInstance.Close()

	nodes, err := clusterconfig.LoadCluster(clusterConfigPath)
	if err != nil {
		return err
	}
	if len(nodes) < config.ReplicationFactor {
		return fmt.Errorf("cluster has %d nodes but replication factor is %d", len(nodes), config.ReplicationFactor)
	}

	found := false
	for _, node := range nodes {
		if node.ID == nodeID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("nodeID %q not found in cluster configuration", nodeID)
	}

	ring := hashring.NewHashRing(config.VirtualNodes, config.ReplicationFactor)
	for _, node := range nodes {
		ring.AddNode(node)
	}

	g := gossip.NewGossip()
	g.SetNodeStatus(nodeID, true)
	workerDone := make([]<-chan struct{}, 0, len(nodes)+3)
	workerDone = append(workerDone, g.StartHeartbeat(ctx, nodeID, nodes))

	for _, node := range nodes {
		if node.ID == nodeID {
			continue
		}
		workerDone = append(workerDone, repair.StartAntiEntropy(ctx, store, walInstance, node.Address))
	}

	handoffDone := make(chan struct{})
	workerDone = append(workerDone, handoffDone)
	go func() {
		defer close(handoffDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				handoffManager.ReplayHints(g, nodes)
			}
		}
	}()

	snapshotDone := make(chan struct{})
	workerDone = append(workerDone, snapshotDone)
	go func() {
		defer close(snapshotDone)
		ticker := time.NewTicker(config.SnapshotInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := snapshot.Save(store, snapshotPath); err != nil {
					logger.Log.Error("snapshot save failed", zap.Error(err))
					continue
				}
				metrics.SnapshotsCreated.Inc()
				logger.Log.Info("snapshot saved", zap.String("file", snapshotPath))
			}
		}
	}()

	server := network.NewServer(address, nodeID, store, walInstance, ring, g, handoffManager)
	server.MaxConnections = maxConnections
	serverErr := server.StartContext(ctx)

	// Stop background workers before taking the final snapshot and closing WAL.
	cancel()
	for _, done := range workerDone {
		<-done
	}
	<-metricsDone

	if err := snapshot.Save(store, snapshotPath); err != nil {
		return fmt.Errorf("save final snapshot %q: %w", snapshotPath, err)
	}
	if err := walInstance.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}
	if serverErr != nil {
		return fmt.Errorf("TCP server: %w", serverErr)
	}
	return nil
}

const networkDefaultMaxConnections = 1024

func intEnvOrDefault(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return parsed, nil
}

func int64EnvOrDefault(key string, fallback int64) (int64, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return parsed, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
