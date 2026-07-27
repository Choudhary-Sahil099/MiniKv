package clusterconfig

import (
	"encoding/json"
	"os"

	"minikv/internal/cluster"
)

type ClusterConfig struct {
	Nodes []cluster.Node `json:"nodes"`
}

func LoadCluster(path string) ([]cluster.Node, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg ClusterConfig

	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return nil, err
	}

	return cfg.Nodes, nil
}