package handoff

import "time"

type Hint struct {
	TargetNode string
	Command    string
	CreatedAt  time.Time
}
