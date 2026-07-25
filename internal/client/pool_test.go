package client

import (
	"net"
	"testing"
)

func startTestServer(t *testing.T) (string, func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()

				buf := make([]byte, 256)
				c.Read(buf)
			}(conn)
		}
	}()

	return ln.Addr().String(), func() {
		ln.Close()
	}
}

func TestGetConnection(t *testing.T) {
	address, cleanup := startTestServer(t)
	defer cleanup()

	pc, err := GetConnection(address)
	if err != nil {
		t.Fatal(err)
	}

	if pc == nil {
		t.Fatal("expected non-nil connection")
	}

	RemoveConnection(address)
}

func TestConnectionReuse(t *testing.T) {
	address, cleanup := startTestServer(t)
	defer cleanup()

	pc1, err := GetConnection(address)
	if err != nil {
		t.Fatal(err)
	}

	pc2, err := GetConnection(address)
	if err != nil {
		t.Fatal(err)
	}

	if pc1 != pc2 {
		t.Fatal("expected pooled connection to be reused")
	}

	RemoveConnection(address)
}

func TestRemoveConnection(t *testing.T) {
	address, cleanup := startTestServer(t)
	defer cleanup()

	pc1, err := GetConnection(address)
	if err != nil {
		t.Fatal(err)
	}

	RemoveConnection(address)

	pc2, err := GetConnection(address)
	if err != nil {
		t.Fatal(err)
	}

	if pc1 == pc2 {
		t.Fatal("expected new connection after removal")
	}

	RemoveConnection(address)
}

func TestConnectionFailure(t *testing.T) {
	_, err := GetConnection("127.0.0.1:65535")

	if err == nil {
		t.Fatal("expected connection error")
	}
}

func TestRemoveNonExistingConnection(t *testing.T) {
	RemoveConnection("127.0.0.1:9999")
}