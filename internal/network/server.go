package network

import (
	"fmt"
	"minikv/internal/gossip"
	"minikv/internal/handoff"
	"minikv/internal/hashring"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"net"
)

type Server struct {
	Address string // tcp node on which the node listens
	NodeID  string //Identifies the current node
	Store   *storage.Store // provides access to local key-value data
	WAL     *wal.WAL //Persists incoming write operations
	Ring    *hashring.HashRing //Determines key ownership and replica placement
	Gossip  *gossip.Gossip //Provides cluster membership and node information
	handoff *handoff.Manager //Manages temporarily stored writes for unavailable nodes
}

func NewServer(
	address string,
	nodeID string,
	store *storage.Store,
	wal *wal.WAL,
	ring *hashring.HashRing,
	g *gossip.Gossip,
	handoff *handoff.Manager,
) *Server {

	return &Server{
		Address: address,
		NodeID:  nodeID,
		Store:   store,
		WAL:     wal,
		Ring:    ring,
		Gossip:  g,
		handoff: handoff,
	}
}

func (s *Server) Start() error {

	listener, err := net.Listen("tcp", s.Address) // start the Tcp server

	if err != nil {
		return err
	}

	defer listener.Close()

	fmt.Println("MiniKV Server Running on", s.Address)

	for {

		conn, err := listener.Accept() //Waits for client connections.

		if err != nil {
			fmt.Println("Connection Error:", err)
			continue
		}

		go HandleConnection(
			conn,
			s.NodeID,
			s.Store,
			s.WAL,
			s.Ring,
			s.Gossip,
			s.handoff,
		)
	}
}
