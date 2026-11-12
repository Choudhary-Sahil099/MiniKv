package handlers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"minikv/internal/cluster"
	"minikv/internal/config"
	"minikv/internal/gossip"
	"minikv/internal/hashring"
	"minikv/internal/network/common"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
	"minikv/internal/wal"
)

func startValuePeer(t *testing.T, value storage.Value, delay time.Duration) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = bufio.NewReader(conn).ReadString('\n')
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-done:
					return
				case <-timer.C:
				}
				payload, _ := json.Marshal(value)
				_, _ = fmt.Fprintf(conn, "%s\n", payload)
			}(conn)
		}
	}()
	return listener.Addr().String(), func() {
		close(done)
		_ = listener.Close()
	}
}

func TestHandleGETReturnsAfterReadQuorumWithoutWaitingForSlowReplica(t *testing.T) {
	clock1 := make(vectorclock.VectorClock)
	clock1["NodeA"] = 1
	clock2 := make(vectorclock.VectorClock)
	clock2["NodeA"] = 2
	clock3 := make(vectorclock.VectorClock)
	clock3["NodeA"] = 3
	now := time.Now().UTC()

	ids := []string{"NodeA", "NodeB", "NodeC"}
	initialRing := hashring.NewHashRing(config.VirtualNodes, config.ReplicationFactor)
	for _, id := range ids {
		initialRing.AddNode(cluster.Node{ID: id, Address: "127.0.0.1:1"})
	}
	owner := initialRing.GetNode("key")
	initialReplicas := initialRing.GetReplicaNodes("key")
	if len(initialReplicas) != 2 {
		t.Fatalf("expected two replicas, got %d", len(initialReplicas))
	}

	fastAddress, stopFast := startValuePeer(t, storage.Value{Data: "fast-quorum-value", CreatedAt: now.Add(time.Second), Clock: clock2}, 0)
	defer stopFast()
	slowAddress, stopSlow := startValuePeer(t, storage.Value{Data: "slow-newest-value", CreatedAt: now.Add(2 * time.Second), Clock: clock3}, 2*time.Second)
	defer stopSlow()

	addresses := map[string]string{owner.ID: "127.0.0.1:1"}
	addresses[initialReplicas[0].ID] = fastAddress
	addresses[initialReplicas[1].ID] = slowAddress
	nodes := make([]cluster.Node, 0, len(ids))
	ring := hashring.NewHashRing(config.VirtualNodes, config.ReplicationFactor)
	for _, id := range ids {
		node := cluster.Node{ID: id, Address: addresses[id]}
		nodes = append(nodes, node)
		ring.AddNode(node)
	}
	if ring.GetNode("key").ID != owner.ID {
		t.Fatal("ring owner changed when peer addresses were assigned")
	}

	store := storage.NewStore()
	store.SetValue("key", storage.Value{Data: "local-old-value", CreatedAt: now, Clock: clock1})
	log, err := wal.NewWAL(filepath.Join(t.TempDir(), "node"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	g := gossip.NewGossip()
	for _, node := range nodes {
		g.SetNodeStatus(node.ID, true)
	}
	ctx := &common.CommandContext{NodeID: owner.ID, Store: store, WAL: log, Ring: ring, Gossip: g, IsForwarded: true}

	start := time.Now()
	response := HandleGET(ctx, "GET key")
	elapsed := time.Since(start)
	if response != "fast-quorum-value" {
		t.Fatalf("GET response = %q, want fast quorum value", response)
	}
	if elapsed >= time.Second {
		t.Fatalf("GET waited for slow replica: elapsed %v", elapsed)
	}
}
