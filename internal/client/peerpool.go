package client

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

//future addition --> context cancellation.

/*
It allows a running operation to stop when the caller no longer needs its result, when a deadline expires, or when the parent operation is cancelled.
*/

// manages reusable TCP connections between MiniKV nodes
const (
	maxConnsPerPeer = 32
	dialTimeout     = 2 * time.Second
	ioTimeout       = 10 * time.Second
	acquireTimeout  = 5 * time.Second
)

type peerConn struct {
	conn        net.Conn
	reader      *bufio.Reader
	discardOnce sync.Once
}

type peerPool struct {
	address string
	slots   chan struct{}
	idle    chan *peerConn
}

var (
	peerPools   = make(map[string]*peerPool)
	peerPoolsMu sync.Mutex
)

// function obtains the pool associated with the destination address.
func getPeerPool(address string) *peerPool {
	peerPoolsMu.Lock()
	defer peerPoolsMu.Unlock()

	p, ok := peerPools[address]
	if !ok {
		p = &peerPool{
			address: address,
			slots:   make(chan struct{}, maxConnsPerPeer),
			idle:    make(chan *peerConn, maxConnsPerPeer),
		}
		peerPools[address] = p
	}
	return p
}


func (p *peerPool) acquire() (*peerConn, error) {
	select {
	case pc := <-p.idle:
		return pc, nil
	default:
	}

	timer := time.NewTimer(acquireTimeout)
	defer timer.Stop()

	select {
	case pc := <-p.idle:
		return pc, nil
	case p.slots <- struct{}{}:
		conn, err := net.DialTimeout("tcp", p.address, dialTimeout)
		if err != nil {
			<-p.slots
			return nil, err
		}
		return &peerConn{conn: conn, reader: bufio.NewReader(conn)}, nil
	case <-timer.C:
		return nil, fmt.Errorf("timeout waiting for connection %s", p.address)
	}
}

func (p *peerPool) release(pc *peerConn) {
	select {
	case p.idle <- pc:
	default:
		p.discard(pc)
	}
}

func (p *peerPool) discard(pc *peerConn) {
	pc.discardOnce.Do(func() {
		pc.conn.Close()
		<-p.slots
	})
}

func (p *peerPool) flushIdle() {
	for {
		select {
		case pc := <-p.idle:
			p.discard(pc)
		default:
			return
		}
	}
}
