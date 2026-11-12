package clusterconfig

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"minikv/internal/cluster"
)

type ClusterConfig struct {
	Nodes []cluster.Node `json:"nodes"`
}

func LoadCluster(path string) ([]cluster.Node, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster config %q: %w", path, err)
	}

	var cfg ClusterConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse cluster config %q: %w", path, err)
	}
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("cluster config %q contains no nodes", path)
	}

	seenIDs := make(map[string]struct{}, len(cfg.Nodes))
	seenAddresses := make(map[string]struct{}, len(cfg.Nodes))
	for i, node := range cfg.Nodes {
		id := strings.TrimSpace(node.ID)
		address := strings.TrimSpace(node.Address)
		if id == "" {
			return nil, fmt.Errorf("cluster config %q: node %d has an empty ID", path, i)
		}
		if strings.ContainsAny(id, ",=\r\n\t ") {
			return nil, fmt.Errorf("cluster config %q: node ID %q contains characters reserved by the vector-clock protocol", path, id)
		}
		if address == "" {
			return nil, fmt.Errorf("cluster config %q: node %q has an empty address", path, id)
		}
		if _, _, err := net.SplitHostPort(address); err != nil {
			return nil, fmt.Errorf("cluster config %q: invalid address %q for node %q: %w", path, address, id, err)
		}
		if _, exists := seenIDs[id]; exists {
			return nil, fmt.Errorf("cluster config %q: duplicate node ID %q", path, id)
		}
		if _, exists := seenAddresses[address]; exists {
			return nil, fmt.Errorf("cluster config %q: duplicate node address %q", path, address)
		}
		seenIDs[id] = struct{}{}
		seenAddresses[address] = struct{}{}
		cfg.Nodes[i].ID = id
		cfg.Nodes[i].Address = address
	}

	return cfg.Nodes, nil
}
