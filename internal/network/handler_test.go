package network

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestHandleConnectionSupportsMultipleCommands(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	go HandleConnection(serverConn, "NodeTest", nil, nil, nil, nil, nil)

	if _, err := io.WriteString(clientConn, "PING\nPING\n"); err != nil {
		t.Fatal(err)
	}
	_ = clientConn.SetReadDeadline(time.Now().Add(time.Second))
	reader := bufio.NewReader(clientConn)
	for i := 0; i < 2; i++ {
		response, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("response %d read failed: %v", i, err)
		}
		if strings.TrimSpace(response) != "PONG" {
			t.Fatalf("response %d = %q, want PONG", i, response)
		}
	}
}

func TestHandleConnectionRejectsOversizedCommand(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	go HandleConnection(serverConn, "NodeTest", nil, nil, nil, nil, nil)

	payload := strings.Repeat("x", maxCommandBytes+1) + "\n"
	writeDone := make(chan error, 1)
	go func() {
		_, err := io.WriteString(clientConn, payload)
		writeDone <- err
	}()

	_ = clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response, err := bufio.NewReader(clientConn).ReadString('\n')
	if err != nil {
		t.Fatalf("oversized-command response read failed: %v", err)
	}
	if strings.TrimSpace(response) != "ERR command too large" {
		t.Fatalf("response = %q", response)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("sending oversized command failed: %v", err)
	}
}
