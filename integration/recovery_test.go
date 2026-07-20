package integration

import (
	"strings"
	"testing"
	"time"
)

func TestCrashRecovery(t *testing.T) {

	CleanupData(t)

	cluster := NewTestCluster(t)
	cluster.Start()

	_, err := SendCommand(
		"localhost:5000",
		"SET apple red",
	)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)

	cluster.Stop()

	cluster.Start()
	defer cluster.Stop()

	resp, err := SendCommand(
		"localhost:5000",
		"LOCAL_GET apple",
	)

	if err != nil {
		t.Fatal(err)
	}

	t.Log(resp)

	if !strings.Contains(resp, "Value=red") {
		t.Fatalf(
			"expected recovered value 'red', got %s",
			resp,
		)
	}
}
