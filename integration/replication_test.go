package integration

import (
	"strings"
	"testing"
	"time"
)

func TestReplication(t *testing.T) {
	cluster := NewTestCluster(t)
	cluster.Start()
	defer cluster.Stop()

	response, err := SendCommand("localhost:5000", "SET apple red")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(response) != "OK" {
		t.Fatalf("SET response = %q, want OK", response)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		found := 0
		for _, node := range cluster.Nodes {
			resp, err := SendCommand(node.Address, "LOCAL_GET apple")
			if err != nil {
				t.Fatalf("read from %s failed: %v", node.ID, err)
			}
			if strings.Contains(resp, "Value=red") {
				found++
			}
		}
		if found == 3 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	var states []string
	for _, node := range cluster.Nodes {
		resp, err := SendCommand(node.Address, "LOCAL_GET apple")
		if err != nil {
			states = append(states, node.ID+": error: "+err.Error())
			continue
		}
		states = append(states, node.ID+": "+strings.TrimSpace(resp))
	}
	t.Fatalf("expected all 3 replicas to contain apple=red; states: %s", strings.Join(states, "; "))
}
