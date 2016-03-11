package sla

import (
	"errors"
)

var (
	ErrNegative  = errors.New("can not be negative")
	ErrEmptyArg  = errors.New("empty argument")
	ErrNotNumber = errors.New("not number")
)
