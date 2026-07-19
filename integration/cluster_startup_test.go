package integration

import (
	"testing"
	"os"
)

func TestClusterStartup(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Working directory: %s", wd)
	cluster := NewTestCluster(t)

	cluster.Start()
	defer cluster.Stop()

	for _, node := range cluster.Nodes {

		_, err := SendCommand(
			node.Address,
			"GET test",
		)

		if err != nil {
			t.Fatalf(
				"failed to communicate with %s: %v",
				node.ID,
				err,
			)
		}
	}
}
