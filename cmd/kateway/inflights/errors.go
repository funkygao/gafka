package inflights

import (
	"errors"
)

var (
	ErrOutOfOrder = errors.New("out of order offset for the same partition")
)
