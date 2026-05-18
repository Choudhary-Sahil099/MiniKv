package main

import (
	"log"

	"minikv/internal/network"
	"minikv/internal/storage"
	"minikv/internal/wal"
)

func main() {

	store := storage.NewStore()

	walInstance, err := wal.NewWAL("data/wal.log")

	if err != nil {
		log.Fatal(err)
	}

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