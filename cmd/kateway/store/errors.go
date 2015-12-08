package store

import (
	"errors"
)

var (
	ErrTooManyConsumers = errors.New("consumers larger than available partitions")
	ErrRebalancing      = errors.New("rebalancing, please come back after a while")
)
