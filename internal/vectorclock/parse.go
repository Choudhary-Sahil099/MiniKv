package vectorclock

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse decodes a serialized vector clock and rejects malformed or ambiguous
// entries instead of silently dropping causal metadata.
func Parse(data string) (VectorClock, error) {
	clock := make(VectorClock)
	if data == "" {
		return clock, nil
	}

	for _, entry := range strings.Split(data, ",") {
		parts := strings.Split(entry, "=")
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid vector clock entry %q", entry)
		}
		nodeID := strings.TrimSpace(parts[0])
		if nodeID != parts[0] {
			return nil, fmt.Errorf("invalid whitespace in vector clock node ID %q", parts[0])
		}
		counter, err := strconv.Atoi(parts[1])
		if err != nil || counter < 0 {
			return nil, fmt.Errorf("invalid vector clock counter in %q", entry)
		}
		if _, exists := clock[nodeID]; exists {
			return nil, fmt.Errorf("duplicate vector clock node %q", nodeID)
		}
		clock[nodeID] = counter
	}
	return clock, nil
}
