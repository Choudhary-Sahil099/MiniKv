package client

import (
	"bufio"
	"net"
	"sync"
	"time"
)

const (
	maxConnsPerPeer = 32
	dialTimeout     = 2 * time.Second
	ioTimeout       = 10 * time.Second
)

type peerConn struct {
	conn   net.Conn
	reader *bufio.Reader
}

type peerPool struct {
	address string
	slots chan struct{}
	idle chan *peerConn
}

var (
	peerPools   = make(map[string]*peerPool)
	peerPoolsMu sync.Mutex
)

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
	pc.conn.Close()
	<-p.slots
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
