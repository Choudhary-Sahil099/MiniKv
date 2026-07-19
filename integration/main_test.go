package integration

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {

	// Build the MiniKV server once before running all tests.
	if err := BuildServer(); err != nil {
		fmt.Println("Failed to build MiniKV test binary:")
		fmt.Println(err)
		os.Exit(1)
	}
	code := m.Run()

	if err := RemoveBinary(); err != nil {
		fmt.Println("Warning: failed to remove test binary:")
		fmt.Println(err)
	}

	os.Exit(code)
}