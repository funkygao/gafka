package store

import (
	"errors"
)

var (
	ErrShuttingDown     = errors.New("server shutting down")
	ErrBusy             = errors.New("server too busy")
	ErrTooManyConsumers = errors.New("consumers larger than available partitions")
	ErrRebalancing      = errors.New("rebalancing, please retry after a while")
	ErrInvalidCluster   = errors.New("invalid cluster")
	ErrEmptyBrokers     = errors.New("empty broker list")
)
