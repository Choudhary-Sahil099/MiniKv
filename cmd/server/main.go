package main

import (
	"fmt"
	"log"
	"os"

	"minikv/internal/cluster"
	"minikv/internal/gossip"
	"minikv/internal/hashring"
	"minikv/internal/network"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"minikv/internal/metrics"
)

func main() {
	metrics.Init()

	go metrics.StartServer()
	if len(os.Args) != 3 {
		log.Fatal("Usage: go run ./cmd/server <NodeID> <Port>")
	}

	nodeID := os.Args[1]
	port := os.Args[2]
	address := "localhost:" + port

	store := storage.NewStore()

	walPath := "data/" + nodeID + ".log"
	err := wal.Recover(store, walPath)
	if err != nil {
		log.Fatal(err)
	}

	walInstance, err := wal.NewWAL(walPath)
	if err != nil {
		log.Fatal(err)
	}

	ring := hashring.NewHashRing(3)
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
	for _, node := range nodes {
		ring.AddNode(node)
	}
	g := gossip.NewGossip()
	g.SetNodeStatus(nodeID, true)
	g.StartHeartbeat(
		nodeID,
		nodes,
	)
	node := ring.GetNode("user123")
	fmt.Println("user123 belongs to:", node.ID)
	fmt.Println("Address:", node.Address)

	server := network.NewServer(
		address,
		nodeID,
		store,
		walInstance,
		ring,
		g,
	)

	err = server.Start()
	if err != nil {
		log.Fatal(err)
	}
}
