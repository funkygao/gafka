package main

import (
	"errors"
)

var (
	ErrTooManyConsumers = errors.New("consumers within a group cannot exceed partition count")
)
