package network

import (
	"fmt"
	"net"
)
import "minikv/internal/storage"

type Server struct {
	Address string
	Store   *storage.Store
}

func NewServer(address string, store *storage.Store) *Server {
	return &Server{
		Address: address,
		Store:   store,
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

		go HandleConnection(conn, s.Store)
	}
}