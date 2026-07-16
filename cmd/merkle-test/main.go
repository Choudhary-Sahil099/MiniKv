package main

import (
	"fmt"

	"minikv/internal/merkle"
	"minikv/internal/storage"
)

func main() {

	data1 := map[string]storage.Value{
		"apple":  {Data: "red"},
		"banana": {Data: "yellow"},
	}

	data2 := map[string]storage.Value{
		"apple":  {Data: "red"},
		"banana": {Data: "yellow"},
		"cat":    {Data: "animal"},
	}
	tree1 := merkle.Build(data1)
	tree2 := merkle.Build(data2)

	diffs := tree1.Differences(tree2)

	fmt.Println("Differences:", len(diffs))

	for _, node := range diffs {
		fmt.Println(node.Key)
	}
}
