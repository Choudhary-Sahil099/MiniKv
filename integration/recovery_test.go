package integration

import (
	"strings"
	"testing"
	"time"
)

func TestCrashRecovery(t *testing.T) {
	cluster := NewTestCluster(t)
	cluster.Start()

	response, err := SendCommand("localhost:5000", "SET apple red")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(response) != "OK" {
		t.Fatalf("SET response = %q, want OK", response)
	}

	// Allow the asynchronous final replica acknowledgement/handoff path to
	// finish before simulating a process crash.
	time.Sleep(500 * time.Millisecond)
	cluster.Stop()

	cluster.Start()
	defer cluster.Stop()

	for _, node := range cluster.Nodes {
		resp, err := SendCommand(node.Address, "LOCAL_GET apple")
		if err != nil {
			t.Fatalf("read from %s after restart failed: %v", node.ID, err)
		}
		if !strings.Contains(resp, "Value=red") {
			t.Fatalf("node %s did not recover the replicated value: %q", node.ID, resp)
		}
	}
}
