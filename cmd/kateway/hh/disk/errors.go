package disk

import (
	"fmt"
)

var (
	ErrNotOpen        = fmt.Errorf("queue not open")
	ErrQueueFull      = fmt.Errorf("queue is full")
	ErrSegmentFull    = fmt.Errorf("segment is full")
	ErrCursorNotFound = fmt.Errorf("cursor not found")
)
