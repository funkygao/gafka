package disk

import (
	"time"
)

const (
	defaultSegmentSize = 10 << 20
	cursorFile         = "cursor.dmp"
	maxBlockSize       = 1 << 20
	initialBackoff     = time.Millisecond * 200
	maxBackoff         = time.Second * 31
	defaultMaxRetries  = 8
	pollEofSleep       = time.Second
	dumpPerBlocks      = 100
)
