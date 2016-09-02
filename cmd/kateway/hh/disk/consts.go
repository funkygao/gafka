package disk

import (
	"time"
)

const (
	defaultSegmentSize = 10 << 20
	cursorFile         = "cursor.dmp"
	maxBlockSize       = 1 << 20
	backoffDuration    = time.Millisecond * 200
	maxRetries         = 5
)
