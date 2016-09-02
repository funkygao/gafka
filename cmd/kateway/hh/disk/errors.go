package disk

import (
	"fmt"
)

var (
	ErrNotOpen        = fmt.Errorf("queue not open")
	ErrQueueOpen      = fmt.Errorf("queue is open")
	ErrQueueFull      = fmt.Errorf("queue is full")
	ErrSegmentCorrupt = fmt.Errorf("segment file corrupted")
	ErrSegmentFull    = fmt.Errorf("segment is full")
	ErrEOQ            = fmt.Errorf("end of queue")
	ErrCursorNotFound = fmt.Errorf("cursor not found")
)
