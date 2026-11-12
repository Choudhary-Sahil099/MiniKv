package integration

import (
	"strings"
	"testing"
	"time"
)

func TestDeleteTombstoneSurvivesReplicaRestart(t *testing.T) {
	cluster := NewTestCluster(t)
	cluster.Start()

	setResponse, err := SendCommand("localhost:5000", "SET delete-me value")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(setResponse) != "OK" {
		t.Fatalf("SET response = %q, want OK", setResponse)
	}

	deleteResponse, err := SendCommand("localhost:5000", "DEL delete-me")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(deleteResponse) != "Deleted" {
		t.Fatalf("DEL response = %q, want Deleted", deleteResponse)
	}

	time.Sleep(500 * time.Millisecond)
	cluster.Stop()
	cluster.Start()
	defer cluster.Stop()

	for _, node := range cluster.Nodes {
		response, err := SendCommand(node.Address, "LOCAL_GET delete-me")
		if err != nil {
			t.Fatalf("read from %s after restart failed: %v", node.ID, err)
		}
		if strings.TrimSpace(response) != "LOCAL_NOT_FOUND" {
			t.Fatalf("node %s resurrected deleted key: %q", node.ID, response)
		}
	}
}
