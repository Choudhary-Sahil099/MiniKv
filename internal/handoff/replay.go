package handoff

import (
	"go.uber.org/zap"

	"minikv/internal/client"
	"minikv/internal/cluster"
	"minikv/internal/gossip"
	"minikv/internal/logger"
)

func (m *Manager) ReplayHints(
	g *gossip.Gossip,
	nodes []cluster.Node,
) {

	hints := m.PendingHints()

	for index, hint := range hints {

		// Skip if node is still down
		if !g.IsAlive(hint.TargetNode) {
			continue
		}

		var targetAddress string

		found := false

		for _, node := range nodes {

			if node.ID == hint.TargetNode {

				targetAddress = node.Address
				found = true
				break
			}
		}

		if !found {
			continue
		}

		response, err := client.ForwardCommand(
			targetAddress,
			hint.Command,
		)

		if err != nil {
			continue
		}

		if response == "REPLICATED" {

			logger.Log.Info(
				"hint replayed",
				zap.String("target", hint.TargetNode),
			)

			m.RemoveHint(index)
		}
	}
}
