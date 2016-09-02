package disk

import (
	"fmt"
)

var (
	ErrNotOpen        = fmt.Errorf("service not open")
	ErrQueueNotOpen   = fmt.Errorf("queue not open")
	ErrQueueOpen      = fmt.Errorf("queue is open")
	ErrQueueFull      = fmt.Errorf("queue is full")
	ErrSegmentNotOpen = fmt.Errorf("segment not open")
	ErrSegmentCorrupt = fmt.Errorf("segment file corrupted")
	ErrSegmentFull    = fmt.Errorf("segment is full")
	ErrEOQ            = fmt.Errorf("end of queue")
	ErrCursorNotFound = fmt.Errorf("cursor not found")
)
