package integration

import (
	"strings"
	"testing"
	"time"
)

func TestReplication(t *testing.T) {

	CleanupData(t)

	cluster := NewTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	_, err := SendCommand(
		"localhost:5000",
		"SET apple red",
	)

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	found := 0

	for _, node := range cluster.Nodes {

		resp, err := SendCommand(
			node.Address,
			"LOCAL_GET apple",
		)

		if err != nil {
			t.Fatal(err)
		}

		t.Logf("%s -> %s", node.ID, resp)

		if strings.Contains(resp, "Value=red") {
			found++
		}
	}

	t.Logf("Replicated copies: %d", found)
}