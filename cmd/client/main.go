package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)
// this file is a command line interface(CLI) for minikv and 

// define the timeout const
const (
	connectionTimeout = 3 * time.Second
	ioTimeout         = 5 * time.Second
)

func writeFull(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func runSession(conn net.Conn, input io.Reader, output, errOutput io.Writer) {
	reader := bufio.NewReader(input)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Fprint(output, "> ")

		cmd, err := reader.ReadString('\n')

		if err != nil && !errors.Is(err, io.EOF) {
			fmt.Fprintln(errOutput, "Input error:", err)
			return
		}

		command := strings.TrimSpace(cmd)

		if strings.EqualFold(command, "EXIT") ||
			strings.EqualFold(command, "QUIT") {
			fmt.Fprintln(output, "Exiting client.")
			return
		}

		if cmd != "" {
			if errors.Is(err, io.EOF) {
				cmd += "\n"
			}

			if err := conn.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
				fmt.Fprintln(errOutput, "Write deadline failed:", err)
				return
			}

			if writeErr := writeFull(conn, []byte(cmd)); writeErr != nil {
				fmt.Fprintln(errOutput, "Write failed:", writeErr)
				return
			}

			if err := conn.SetReadDeadline(time.Now().Add(ioTimeout)); err != nil {
				fmt.Fprintln(errOutput, "Read deadline failed:", err)
				return
			}

			response, readErr := serverReader.ReadString('\n')
			if readErr != nil {
				fmt.Fprintln(errOutput, "Response read failed:", readErr)
				return
			}

			fmt.Fprint(output, response)
		}

		if errors.Is(err, io.EOF) {
			fmt.Fprintln(output, "\nExiting client.")
			return
		}
	}
}

// tcp -> 16_bit unsigned integer. The valid range configured is 1-65535
/*{
	valid and unvalid;
	abc(not valid, 0 or less than 0)
	numbers between 1 and 65535 inclusive
}*/
func parsePort(args []string) (int, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("expected exactly one port argument")
	}

	port, err := strconv.Atoi(args[0])
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("port must be between 1 and 65535")
	}

	return port, nil
}

func main() {
	// os.args --> handle command line args
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: go run cmd/client/main.go <port>")
		os.Exit(1)
	}
	
	// parse port
	port, err := parsePort(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "Usage: go run cmd/client/main.go <port>")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	address := net.JoinHostPort("localhost", strconv.Itoa(port))

	// used for the TCP connection establishment
	// originally used net.Dial then improved it by using the DialTimeout
	conn, err := net.DialTimeout("tcp", address, connectionTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Connection failed (%s): %v\n", address, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to", address)
	runSession(conn, os.Stdin, os.Stdout, os.Stderr)
}
