package disk

import (
	"fmt"
)

var (
	ErrNotOpen        = fmt.Errorf("queue not open")
	ErrQueueOpen      = fmt.Errorf("queue is open")
	ErrQueueFull      = fmt.Errorf("queue is full")
	ErrSegmentFull    = fmt.Errorf("segment is full")
	ErrCursorNotFound = fmt.Errorf("cursor not found")
)
