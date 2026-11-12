package network

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func reserveTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func startTestServer(t *testing.T, maxConnections int) (*Server, context.CancelFunc, <-chan error) {
	t.Helper()
	server := NewServer(reserveTCPAddress(t), "NodeTest", nil, nil, nil, nil, nil)
	server.MaxConnections = maxConnections
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.StartContext(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		started := server.listener != nil
		server.mu.Unlock()
		if started {
			return server, cancel, done
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	t.Fatal("server did not start listening")
	return nil, nil, nil
}

func TestServerStopsOnContextCancellation(t *testing.T) {
	server, cancel, done := startTestServer(t, 4)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StartContext() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop after context cancellation")
	}
	if conn, err := net.DialTimeout("tcp", server.Address, 100*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatal("listener remained open after shutdown")
	}
}

func TestServerRejectsConnectionsAboveLimit(t *testing.T) {
	server, cancel, done := startTestServer(t, 1)
	defer func() {
		cancel()
		<-done
	}()

	first, err := net.DialTimeout("tcp", server.Address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := first.Write([]byte("PING\n")); err != nil {
		t.Fatal(err)
	}
	first.SetReadDeadline(time.Now().Add(time.Second))
	if response, err := bufio.NewReader(first).ReadString('\n'); err != nil || strings.TrimSpace(response) != "PONG" {
		t.Fatalf("first connection response = %q, err = %v", response, err)
	}

	second, err := net.DialTimeout("tcp", server.Address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := bufio.NewReader(second).ReadString('\n'); err == nil {
		t.Fatal("second connection should be rejected while the limit is reached")
	}
}

func TestServerShutdownClosesActiveConnections(t *testing.T) {
	server, cancel, done := startTestServer(t, 2)
	conn, err := net.DialTimeout("tcp", server.Address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not wait for active connection cleanup")
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := bufio.NewReader(conn).ReadString('\n'); err == nil {
		t.Fatal("active connection was not closed during shutdown")
	}
}
