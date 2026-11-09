package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// mockConn  --> allows us to simulate different write behaviours
type mockConn struct {
	writeFunc func([]byte) (int, error)
}

func (m *mockConn) Write(p []byte) (int, error) {
	return m.writeFunc(p)
}

func (m *mockConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m *mockConn) SetDeadline(time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(time.Time) error {
	return nil
}

func TestWriteFull(t *testing.T) {
	var output bytes.Buffer

	conn := &mockConn{
		writeFunc: func(p []byte) (int, error) {
			return output.Write(p)
		},
	}

	data := []byte("SET name sahil\n")

	err := writeFull(conn, data)
	if err != nil {
		t.Fatalf("writeFull returned unexpected error: %v", err)
	}

	if !bytes.Equal(output.Bytes(), data) {
		t.Fatalf("expected %q, got %q", data, output.Bytes())
	}
}

func TestWriteFullPartialWrite(t *testing.T) {
	var output bytes.Buffer

	conn := &mockConn{
		writeFunc: func(p []byte) (int, error) {
			if len(p) > 3 {
				p = p[:3]
			}
			return output.Write(p)
		},
	}

	data := []byte("SET key value\n")

	err := writeFull(conn, data)
	if err != nil {
		t.Fatalf("writeFull returned unexpected error: %v", err)
	}

	if !bytes.Equal(output.Bytes(), data) {
		t.Fatalf("expected %q, got %q", data, output.Bytes())
	}
}

func TestWriteFullZeroWrite(t *testing.T) {
	conn := &mockConn{
		writeFunc: func([]byte) (int, error) {
			return 0, nil
		},
	}

	err := writeFull(conn, []byte("SET key value\n"))

	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("expected io.ErrShortWrite, got %v", err)
	}
}

func TestWriteFullWriteError(t *testing.T) {
	expectedErr := errors.New("simulated connection failure")

	conn := &mockConn{
		writeFunc: func([]byte) (int, error) {
			return 0, expectedErr
		},
	}

	err := writeFull(conn, []byte("SET key value\n"))

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

func TestRunSessionServerDisconnect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	serverDone := make(chan error, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		_, err = reader.ReadString('\n')
		serverDone <- err

		// Close without sending a response.
	}()

	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer clientConn.Close()

	var output bytes.Buffer
	var errOutput bytes.Buffer

	runSession(
		clientConn,
		strings.NewReader("GET missing\n"),
		&output,
		&errOutput,
	)

	if err := <-serverDone; err != nil {
		t.Fatalf("server failed to receive command: %v", err)
	}

	if !strings.Contains(errOutput.String(), "Response read failed:") {
		t.Fatalf(
			"expected response read error, got: %q",
			errOutput.String(),
		)
	}
}
func TestRunSessionResponseTimeout(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	var output bytes.Buffer
	var errOutput bytes.Buffer

	releaseServer := make(chan struct{})
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		defer serverConn.Close()

		reader := bufio.NewReader(serverConn)
		_, _ = reader.ReadString('\n')

		// Keep the connection open without responding.
		<-releaseServer
	}()

	start := time.Now()

	runSession(
		clientConn,
		strings.NewReader("GET missing\n"),
		&output,
		&errOutput,
	)

	elapsed := time.Since(start)

	close(releaseServer)
	<-serverDone

	if !strings.Contains(errOutput.String(), "Response read failed:") {
		t.Fatalf(
			"expected response read failure, got: %q",
			errOutput.String(),
		)
	}

	if elapsed < ioTimeout {
		t.Fatalf(
			"expected timeout after at least %v, got %v",
			ioTimeout,
			elapsed,
		)
	}
}

func TestRunSessionWriteTimeout(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	var output bytes.Buffer
	var errOutput bytes.Buffer

	releaseServer := make(chan struct{})
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)
		defer serverConn.Close()

		// Keep the connection open without reading.
		<-releaseServer
	}()

	start := time.Now()

	runSession(
		clientConn,
		strings.NewReader("SET key value\n"),
		&output,
		&errOutput,
	)

	elapsed := time.Since(start)

	close(releaseServer)
	<-serverDone

	if !strings.Contains(errOutput.String(), "Write failed:") {
		t.Fatalf(
			"expected write failure, got: %q",
			errOutput.String(),
		)
	}

	if elapsed < ioTimeout {
		t.Fatalf(
			"expected timeout after at least %v, got %v",
			ioTimeout,
			elapsed,
		)
	}
}

func TestRunSessionFinalInputWithoutNewline(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	var output bytes.Buffer
	var errOutput bytes.Buffer

	serverDone := make(chan error, 1)

	go func() {
		reader := bufio.NewReader(serverConn)

		command, err := reader.ReadString('\n')
		if err != nil {
			serverDone <- err
			return
		}

		if command != "GET key\n" {
			serverDone <- fmt.Errorf(
				"expected GET key\\n, got %q", command,
			)
			return
		}

		_, err = serverConn.Write([]byte("value\n"))
		serverDone <- err
	}()

	runSession(
		clientConn,
		strings.NewReader("GET key"),
		&output,
		&errOutput,
	)

	if err := <-serverDone; err != nil {
		t.Fatalf("server interaction failed: %v", err)
	}

	if !strings.Contains(output.String(), "value") {
		t.Fatalf("expected response value, got %q", output.String())
	}

	if !strings.Contains(output.String(), "Exiting client.") {
		t.Fatalf("expected clean EOF exit, got %q", output.String())
	}
}

type failingReader struct{}

func (f failingReader) Read([]byte) (int, error) {
	return 0, errors.New("simulated input failure")
}

func TestRunSessionInputError(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	var output bytes.Buffer
	var errOutput bytes.Buffer

	runSession(
		clientConn,
		failingReader{},
		&output,
		&errOutput,
	)

	if !strings.Contains(errOutput.String(), "Input error:") {
		t.Fatalf(
			"expected input error message, got %q",
			errOutput.String(),
		)
	}
}

func TestRunSessionExitCommands(t *testing.T) {
	commands := []string{
		"EXIT\n",
		"exit\n",
		"Exit\n",
		"QUIT\n",
		"quit\n",
		"Quit\n",
	}

	for _, command := range commands {
		t.Run(strings.TrimSpace(command), func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			var output bytes.Buffer
			var errOutput bytes.Buffer

			runSession(
				clientConn,
				strings.NewReader(command),
				&output,
				&errOutput,
			)

			if !strings.Contains(output.String(), "Exiting client.") {
				t.Fatalf("expected exit message, got %q", output.String())
			}

			if errOutput.Len() != 0 {
				t.Fatalf("unexpected error: %q", errOutput.String())
			}
		})
	}
}

func TestParsePort(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    int
		wantErr bool
	}{
		{"missing argument", []string{}, 0, true},
		{"multiple arguments", []string{"5000", "5001"}, 0, true},
		{"non-numeric port", []string{"abc"}, 0, true},
		{"zero port", []string{"0"}, 0, true},
		{"negative port", []string{"-1"}, 0, true},
		{"port above maximum", []string{"65536"}, 0, true},
		{"minimum valid port", []string{"1"}, 1, false},
		{"node A port", []string{"5000"}, 5000, false},
		{"node B port", []string{"5001"}, 5001, false},
		{"maximum valid port", []string{"65535"}, 65535, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePort(tt.args)

			if (err != nil) != tt.wantErr {
				t.Fatalf("parsePort() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil && got != tt.want {
				t.Fatalf("parsePort() = %d, want %d", got, tt.want)
			}
		})
	}
}
