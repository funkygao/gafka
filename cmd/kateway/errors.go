package main

import (
	"errors"
)

var (
	ErrTooManyConsumers = errors.New("consumers within a group cannot exceed partition count")
	ErrRebalancing      = errors.New("rebalancing, please come back after a while")
)
