package network

import (
	"fmt"
	"minikv/internal/gossip"
	"minikv/internal/hashring"
	"minikv/internal/storage"
	"minikv/internal/wal"
	"minikv/internal/handoff"
	"net"
)

type Server struct {
	Address string
	NodeID  string
	Store   *storage.Store
	WAL     *wal.WAL
	Ring    *hashring.HashRing
	Gossip  *gossip.Gossip
	handoff *handoff.Manager
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
