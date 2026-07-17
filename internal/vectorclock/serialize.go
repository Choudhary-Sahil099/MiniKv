package vectorclock

import (
	"sort"
	"strconv"
	"strings"
)

func (vc VectorClock) Serialize() string {

	if len(vc) == 0 {
		return ""
	}

	keys := make([]string, 0, len(vc))

	for node := range vc {
		keys = append(keys, node)
	}

	sort.Strings(keys)

	var parts []string

	for _, node := range keys {
		parts = append(parts,
			node+"="+strconv.Itoa(vc[node]))
	}

	return strings.Join(parts, ",")
}