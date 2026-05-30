package client

import (
	"bufio"
	"net"
	"sync"
)

type PooledConn struct {
	Conn   net.Conn
	Reader *bufio.Reader
	Mu     sync.Mutex
}

var (
	connPool = make(map[string]*PooledConn)
	mu       sync.Mutex
)

func GetConnection(
	address string,
) (*PooledConn, error) {

	mu.Lock()
	defer mu.Unlock()

	// Return existing connection if available
	if pc, exists := connPool[address]; exists {
		return pc, nil
	}

	// Create new TCP connection
	conn, err := net.Dial(
		"tcp",
		address,
	)

	if err != nil {
		return nil, err
	}

	pc := &PooledConn{
		Conn:   conn,
		Reader: bufio.NewReader(conn),
	}

	connPool[address] = pc

	return pc, nil
}

func RemoveConnection(address string) {

	mu.Lock()
	defer mu.Unlock()

	if pc, exists := connPool[address]; exists {

		pc.Conn.Close()

		delete(
			connPool,
			address,
		)
	}
}