package main

import (
	"fmt"

	"minikv/internal/merkle"
	"minikv/internal/storage"
)

func main() {

	data := map[string]storage.Value{
		"cat": {
			Data: "animal",
		},
		"apple": {
			Data: "green",
		},
		"banana": {
			Data: "yellow",
		},
	}

	tree := merkle.Build(data)

	fmt.Println(tree.RootHash())
}
