package client

import (
	"bufio"
	"errors"
	"net"
	"strings"
	"testing"
)

func TestPingSuccess(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		request, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		if request != "PING\n" {
			t.Errorf("expected PING, got %q", request)
			return
		}

		_, _ = conn.Write([]byte("PONG\n"))
	}()

	if err := Ping(listener.Addr().String()); err != nil {
		t.Fatalf("expected successful ping, got: %v", err)
	}
}

func TestPingUnexpectedResponse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_, _ = bufio.NewReader(conn).ReadString('\n')
		_, _ = conn.Write([]byte("NOT_PONG\n"))
	}()

	err = Ping(listener.Addr().String())
	if err == nil {
		t.Fatal("expected error for unexpected response")
	}
	if !strings.Contains(err.Error(), "unexpected ping response") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPingConnectionClosedWithoutResponse(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_, _ = bufio.NewReader(conn).ReadString('\n')
		// Closing without sending PONG simulates a failed peer.
	}()

	err = Ping(listener.Addr().String())
	if err == nil {
		t.Fatal("expected error when peer closes without responding")
	}
}

func TestPingTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_, _ = bufio.NewReader(conn).ReadString('\n')
		// Keep the connection open but never send a response.
		// Ping's deadline should cause the client to time out.
		_, _ = bufio.NewReader(conn).ReadString('\n')
	}()

	err = Ping(listener.Addr().String())
	if err == nil {
		t.Fatal("expected ping timeout")
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected timeout error, got: %v", err)
	}
}

func TestPingConnectionFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	err = Ping(address)
	if err == nil {
		t.Fatal("expected connection error")
	}
}
