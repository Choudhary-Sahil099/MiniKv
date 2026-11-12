package clusterconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadClusterValidConfig(t *testing.T) {
	path := writeConfig(t, `{"nodes":[{"id":"NodeA","address":"127.0.0.1:5000"},{"id":"NodeB","address":"127.0.0.1:5001"}]}`)
	nodes, err := LoadCluster(path)
	if err != nil {
		t.Fatalf("LoadCluster() error = %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestLoadClusterRejectsEmptyCluster(t *testing.T) {
	path := writeConfig(t, `{"nodes":[]}`)
	if _, err := LoadCluster(path); err == nil {
		t.Fatal("expected empty cluster to be rejected")
	}
}

func TestLoadClusterRejectsDuplicateIDs(t *testing.T) {
	path := writeConfig(t, `{"nodes":[{"id":"NodeA","address":"127.0.0.1:5000"},{"id":"NodeA","address":"127.0.0.1:5001"}]}`)
	if _, err := LoadCluster(path); err == nil {
		t.Fatal("expected duplicate node IDs to be rejected")
	}
}

func TestLoadClusterRejectsDuplicateAddresses(t *testing.T) {
	path := writeConfig(t, `{"nodes":[{"id":"NodeA","address":"127.0.0.1:5000"},{"id":"NodeB","address":"127.0.0.1:5000"}]}`)
	if _, err := LoadCluster(path); err == nil {
		t.Fatal("expected duplicate node addresses to be rejected")
	}
}

func TestLoadClusterRejectsInvalidAddress(t *testing.T) {
	path := writeConfig(t, `{"nodes":[{"id":"NodeA","address":"localhost"}]}`)
	if _, err := LoadCluster(path); err == nil {
		t.Fatal("expected invalid address to be rejected")
	}
}

func TestLoadClusterRejectsEmptyNodeID(t *testing.T) {
	path := writeConfig(t, `{"nodes":[{"id":"  ","address":"127.0.0.1:5000"}]}`)
	if _, err := LoadCluster(path); err == nil {
		t.Fatal("expected empty node ID to be rejected")
	}
}
