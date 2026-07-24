package hashring

import (
	"minikv/internal/cluster"
	"sort"
	"testing"
)

func createTestRing() *HashRing {
	ring := NewHashRing(10, 3)

	ring.AddNode(cluster.Node{
		ID:      "NodeA",
		Address: "localhost:5000",
	})

	ring.AddNode(cluster.Node{
		ID:      "NodeB",
		Address: "localhost:5001",
	})

	ring.AddNode(cluster.Node{
		ID:      "NodeC",
		Address: "localhost:5002",
	})

	return ring
}

func TestNewHashRing(t *testing.T) {

	ring := NewHashRing(10, 3)

	if ring == nil {
		t.Fatal("ring is nil")
	}

	if len(ring.nodes) != 0 {
		t.Fatal("nodes map should be empty")
	}

	if len(ring.keys) != 0 {
		t.Fatal("keys should be empty")
	}

	if ring.virtualNodes != 10 {
		t.Fatal("virtual node count incorrect")
	}

	if ring.replicationFactor != 3 {
		t.Fatal("replication factor incorrect")
	}
}

func TestGetNodeEmptyRing(t *testing.T) {

	ring := NewHashRing(10, 3)

	node := ring.GetNode("apple")

	if node != (cluster.Node{}) {
		t.Fatal("expected empty node")
	}
}

func TestAddNode(t *testing.T) {

	ring := NewHashRing(5, 2)

	ring.AddNode(cluster.Node{
		ID:      "NodeA",
		Address: "localhost:5000",
	})

	if len(ring.keys) != 5 {
		t.Fatalf("expected 5 virtual nodes got %d", len(ring.keys))
	}

	if len(ring.nodes) != 5 {
		t.Fatalf("expected 5 hashes got %d", len(ring.nodes))
	}
}

func TestKeysAreSorted(t *testing.T) {

	ring := createTestRing()

	if !sort.SliceIsSorted(ring.keys, func(i, j int) bool {
		return ring.keys[i] < ring.keys[j]
	}) {
		t.Fatal("ring keys are not sorted")
	}
}

func TestDeterministicMapping(t *testing.T) {

	ring := createTestRing()

	node1 := ring.GetNode("apple")
	node2 := ring.GetNode("apple")

	if node1.ID != node2.ID {
		t.Fatal("same key mapped to different nodes")
	}
}

func TestGetNodeReturnsValidNode(t *testing.T) {

	ring := createTestRing()

	keys := []string{
		"apple",
		"banana",
		"orange",
		"grape",
		"watermelon",
	}

	valid := map[string]bool{
		"NodeA": true,
		"NodeB": true,
		"NodeC": true,
	}

	for _, key := range keys {

		node := ring.GetNode(key)

		if !valid[node.ID] {
			t.Fatalf("invalid node returned for key %s", key)
		}
	}
}

func TestReplicaDifferentFromPrimary(t *testing.T) {

	ring := createTestRing()

	primary := ring.GetNode("apple")
	replica := ring.GetReplicaNode("apple")

	if primary.ID == replica.ID {
		t.Fatal("replica should not equal primary")
	}
}

func TestReplicaCount(t *testing.T) {

	ring := createTestRing()

	replicas := ring.GetReplicaNodes("apple")

	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas got %d", len(replicas))
	}
}

func TestReplicaUniqueness(t *testing.T) {

	ring := createTestRing()

	replicas := ring.GetReplicaNodes("apple")

	seen := make(map[string]bool)

	for _, node := range replicas {

		if seen[node.ID] {
			t.Fatal("duplicate replica found")
		}

		seen[node.ID] = true
	}
}

func TestReplicaDoesNotContainOwner(t *testing.T) {

	ring := createTestRing()

	owner := ring.GetNode("apple")
	replicas := ring.GetReplicaNodes("apple")

	for _, replica := range replicas {

		if replica.ID == owner.ID {
			t.Fatal("owner returned as replica")
		}
	}
}