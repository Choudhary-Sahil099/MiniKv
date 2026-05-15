package main

import (
	"log"
	"minikv/internal/network"
)
import "minikv/internal/storage"
func main() {

	store := storage.NewStore()

	server := network.NewServer(":5000", store)

	err := server.Start()

	if err != nil {
		log.Fatal(err)
	}
}
