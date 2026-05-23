package main

import (
	"fmt"
	"log"

	"minikv/internal/hashring"
	"minikv/internal/network"
	"minikv/internal/storage"
	"minikv/internal/wal"
)

func main() {

	store := storage.NewStore()

	err := wal.Recover(store, "data/wal.log")

	if err != nil {
		log.Fatal(err)
	}

	walInstance, err := wal.NewWAL("data/wal.log")

	if err != nil {
		log.Fatal(err)
	}

	// HASH RING TEST
	ring := hashring.NewHashRing(3)

	ring.AddNode("NodeA")
	ring.AddNode("NodeB")
	ring.AddNode("NodeC")

	fmt.Println("user123 ->", ring.GetNode("user123"))
	fmt.Println("session456 ->", ring.GetNode("session456"))
	fmt.Println("cache789 ->", ring.GetNode("cache789"))

	server := network.NewServer(
		":5000",
		store,
		walInstance,
	)

	err = server.Start()

	if err != nil {
		log.Fatal(err)
	}
}