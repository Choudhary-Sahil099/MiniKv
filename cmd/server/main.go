package main

import (
	"fmt"
	"log"
	"os"

	"minikv/internal/cluster"
	"minikv/internal/hashring"
	"minikv/internal/network"
	"minikv/internal/storage"
	"minikv/internal/wal"
)

func main() {
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
	ring.AddNode(cluster.Node{ID: "NodeA", Address: "localhost:5000"})
	ring.AddNode(cluster.Node{ID: "NodeB", Address: "localhost:5001"})
	ring.AddNode(cluster.Node{ID: "NodeC", Address: "localhost:5002"})

	node := ring.GetNode("user123")
	fmt.Println("user123 belongs to:", node.ID)
	fmt.Println("Address:", node.Address)

	server := network.NewServer(
		address,
		nodeID,
		store,
		walInstance,
		ring,
	)

	err = server.Start()
	if err != nil {
		log.Fatal(err)
	}
}