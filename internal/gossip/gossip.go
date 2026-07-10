package gossip

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"minikv/internal/client"
	"minikv/internal/cluster"
	"minikv/internal/logger"
	"minikv/internal/metrics"
	"minikv/internal/config"

)

type Gossip struct {
	nodes map[string]bool
	mu    sync.RWMutex
}

func NewGossip() *Gossip {
	return &Gossip{
		nodes: make(map[string]bool),
	}
}

func (g *Gossip) SetNodeStatus(
	nodeID string,
	alive bool,
) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.nodes[nodeID] = alive
}

func (g *Gossip) IsAlive(nodeID string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.nodes[nodeID]
}

func (g *Gossip) StartHeartbeat(
	currentNode string,
	nodes []cluster.Node,
) {

	go func() {

		for {

			alive := 1
			dead := 0

			for _, node := range nodes {

				if node.ID == currentNode {
					continue
				}

				previousStatus := g.IsAlive(node.ID)

				err := client.Ping(
					node.Address,
				)

				if err != nil {

					if previousStatus {
						logger.Log.Warn(
							"node failed",
							zap.String("node", node.ID),
						)
					}

					g.SetNodeStatus(
						node.ID,
						false,
					)

					dead++

				} else {

					if !previousStatus {

						metrics.NodeRecoveries.Inc()

						logger.Log.Info(
							"node recovered",
							zap.String("node", node.ID),
						)
					}

					g.SetNodeStatus(
						node.ID,
						true,
					)

					alive++
				}
			}

			metrics.AliveNodes.Set(
				float64(alive),
			)

			metrics.DeadNodes.Set(
				float64(dead),
			)

			time.Sleep(config.HeartbeatInterval)
		}
	}()
}
