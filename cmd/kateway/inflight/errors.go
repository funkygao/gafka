package inflight

import (
	"errors"
)

var (
	ErrOutOfOrder = errors.New("out of order inflight offset")
)
