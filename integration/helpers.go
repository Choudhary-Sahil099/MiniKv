package integration

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type TestNode struct {
	ID      string
	Port    string
	Address string
	Cmd     *exec.Cmd
}

type TestCluster struct {
	Nodes   []TestNode
	T       *testing.T
	DataDir string
}

// the given test ports
var TestNodes = []TestNode{
	{
		ID:      "NodeA",
		Port:    "5000",
		Address: "localhost:5000",
	},
	{
		ID:      "NodeB",
		Port:    "5001",
		Address: "localhost:5001",
	},
	{
		ID:      "NodeC",
		Port:    "5002",
		Address: "localhost:5002",
	},
}

func BuildServer() error {

	cmd := exec.Command(
		"go",
		"build",
		"-o",
		"minikv-test.exe",
		"./cmd/server",
	)

	cmd.Dir = ".."

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"build failed:\n%s\n%w",
			string(output),
			err,
		)
	}

	return nil
}

func NewTestCluster(t *testing.T) *TestCluster {

	nodes := make([]TestNode, len(TestNodes))
	copy(nodes, TestNodes)

	return &TestCluster{
		Nodes:   nodes,
		T:       t,
		DataDir: t.TempDir(),
	}
}
func WaitForPort(address string, timeout time.Duration) error {

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {

		conn, err := net.DialTimeout(
			"tcp",
			address,
			500*time.Millisecond,
		)

		if err == nil {
			conn.Close()
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timed out waiting for %s", address)
}
func RemoveBinary() error {

	err := os.Remove(filepath.Join("..", "minikv-test.exe"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (c *TestCluster) StartNode(node *TestNode) error {

	cmd := exec.Command(
		"./minikv-test.exe",
		node.ID,
		node.Port,
	)
	cmd.Dir = ".."
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	cmd.Env = append(os.Environ(), "MINIKV_DATA_DIR="+c.DataDir)
	if err := cmd.Start(); err != nil {
		return err
	}

	node.Cmd = cmd

	return WaitForPort(
		node.Address,
		10*time.Second,
	)
}

func (c *TestCluster) Stop() {

	for i := range c.Nodes {
		c.StopNode(&c.Nodes[i])
	}
}

func (c *TestCluster) Start() {

	for i := range c.Nodes {

		err := c.StartNode(&c.Nodes[i])

		if err != nil {
			c.Stop()
			c.T.Fatalf(
				"failed to start %s: %v",
				c.Nodes[i].ID,
				err,
			)
		}
	}
}

func (c *TestCluster) StopNode(
	node *TestNode,
) {

	if node.Cmd == nil || node.Cmd.Process == nil {
		return
	}

	_ = node.Cmd.Process.Kill()
	_ = node.Cmd.Wait()

	node.Cmd = nil
}

func SendCommand(
	address string,
	command string,
) (string, error) {
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return "", err
	}

	payload := []byte(command + "\n")
	for len(payload) > 0 {
		n, writeErr := conn.Write(payload)
		if writeErr != nil {
			return "", writeErr
		}
		if n == 0 {
			return "", fmt.Errorf("short write sending command to %s", address)
		}
		payload = payload[n:]
	}

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}
	return response, nil
}
