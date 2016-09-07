package store

import (
	"errors"
)

var (
	ErrShuttingDown     = errors.New("server shutting down")
	ErrBusy             = errors.New("underlying store too busy")
	ErrTooManyConsumers = errors.New("consumers more than available partitions")
	ErrRebalancing      = errors.New("rebalancing, please retry after a while")
	ErrInvalidTopic     = errors.New("invalid topic")
	ErrInvalidCluster   = errors.New("invalid cluster")
	ErrEmptyBrokers     = errors.New("empty broker list")
	ErrCircuitOpen      = errors.New("circuit open, underlying store problems")
)
