package gossip

import (
	"fmt"
	"sync"
	"time"

	"minikv/internal/cluster"
	"minikv/internal/client"
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

			for _, node := range nodes {

				if node.ID == currentNode {
					continue
				}

				_, err := client.ForwardCommand(
					node.Address,
					"PING",
				)

				if err != nil {

					g.SetNodeStatus(
						node.ID,
						false,
					)
					fmt.Println(node.ID, "is dead")

				} else {

					g.SetNodeStatus(
						node.ID,
						true,
					)
					fmt.Println(node.ID, "is alive")
				}
			}

			time.Sleep(5 * time.Second)
		}
	}()
}