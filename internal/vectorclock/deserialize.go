package vectorclock

import (
	"strconv"
	"strings"
)

func Deserialize(data string) VectorClock {

	clock := make(VectorClock)

	if data == "" {
		return clock
	}

	entries := strings.Split(data, ",")

	for _, entry := range entries {

		parts := strings.Split(entry, "=")

		if len(parts) != 2 {
			continue
		}

		counter, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}

		clock[parts[0]] = counter
	}

	return clock
}