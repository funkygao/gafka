package main

import (
	"errors"
)

var (
	// FIXME dup with store/kafka
	ErrTooManyConsumers = errors.New("consumers within a group cannot exceed partition count")
	ErrRebalancing      = errors.New("rebalancing, please come back after a while")
)
