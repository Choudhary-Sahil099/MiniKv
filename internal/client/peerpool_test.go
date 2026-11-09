
package client

import (
	"bufio"
	"net"
	"strings"
	"testing"
)

func TestPeerPoolAcquireRelease(t *testing.T) {
	address, cleanup := startPeerTestServer(t)
	defer cleanup()

	pool := getPeerPool(address)

	pc1, err := pool.acquire()
	if err != nil {
		t.Fatal(err)
	}

	pool.release(pc1)

	pc2, err := pool.acquire()
	if err != nil {
		t.Fatal(err)
	}

	if pc1 != pc2 {
		t.Fatal("expected released connection to be reused")
	}

	pool.discard(pc2)
}

func TestPeerPoolDiscard(t *testing.T) {
	address, cleanup := startPeerTestServer(t)
	defer cleanup()

	pool := getPeerPool(address)

	pc, err := pool.acquire()
	if err != nil {
		t.Fatal(err)
	}

	pool.discard(pc)

	if len(pool.slots) != 0 {
		t.Fatalf("expected all slots released, got %d", len(pool.slots))
	}

	if _, err := pc.conn.Write([]byte("test")); err == nil {
		t.Fatal("expected discarded connection to be closed")
	}
}

func TestForwardCommandPersistentConnection(t *testing.T) {
	address, cleanup := startPeerTestServer(t)
	defer cleanup()

	response1, err := ForwardCommand(address, "GET first")
	if err != nil {
		t.Fatal(err)
	}

	response2, err := ForwardCommand(address, "GET second")
	if err != nil {
		t.Fatal(err)
	}

	if response1 != "OK:GET first" {
		t.Fatalf("unexpected first response: %s", response1)
	}

	if response2 != "OK:GET second" {
		t.Fatalf("unexpected second response: %s", response2)
	}
}

func startPeerTestServer(t *testing.T) (string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go func(conn net.Conn) {
				defer conn.Close()

				reader := bufio.NewReader(conn)

				for {
					message, err := reader.ReadString('\n')
					if err != nil {
						return
					}

					message = strings.TrimSpace(message)
					_, err = conn.Write([]byte("OK:" + strings.TrimPrefix(message, "FORWARDED ") + "\n"))
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return listener.Addr().String(), func() {
		listener.Close()
	}
}
func TestForwardReadCommandRecoversFromStaleConnection(t *testing.T) {
    listener, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        t.Fatal(err)
    }
    defer listener.Close()

    firstConnectionClosed := make(chan struct{})

    go func() {
        // First connection: respond, then close it.
        conn, err := listener.Accept()
        if err != nil {
            return
        }

        reader := bufio.NewReader(conn)
        _, err = reader.ReadString('\n')
        if err != nil {
            conn.Close()
            return
        }

        conn.Write([]byte("FIRST_RESPONSE\n"))
        conn.Close()
        close(firstConnectionClosed)

        // Second connection: handle the retry.
        conn, err = listener.Accept()
        if err != nil {
            return
        }
        defer conn.Close()

        reader = bufio.NewReader(conn)
        _, err = reader.ReadString('\n')
        if err != nil {
            return
        }

        conn.Write([]byte("RETRY_SUCCESS\n"))
    }()

    address := listener.Addr().String()

    // Prime the pool and let the server close the connection.
    response, err := ForwardCommand(address, "GET first")
    if err != nil {
        t.Fatal(err)
    }
    if response != "FIRST_RESPONSE" {
        t.Fatalf("unexpected response: %s", response)
    }

    <-firstConnectionClosed

    response, err = ForwardReadCommand(address, "GET second")
    if err != nil {
        t.Fatalf("expected retry to succeed: %v", err)
    }

    if response != "RETRY_SUCCESS" {
        t.Fatalf("unexpected retry response: %s", response)
    }
}