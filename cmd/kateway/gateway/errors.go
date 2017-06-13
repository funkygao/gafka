package gateway

import (
	"errors"
)

var (
	ErrClientGone           = errors.New("remote client gone")
	ErrTooBigMessage        = errors.New("too big message")
	ErrTooSmallMessage      = errors.New("too small message")
	ErrIllegalTaggedMessage = errors.New("illegal tagged message")
	ErrClientKilled         = errors.New("client killed")
	ErrBadResponseWriter    = errors.New("ResponseWriter Close not supported")
	ErrPartitionOutOfRange  = errors.New("partition out of range")
	ErrOffsetOutOfRange     = errors.New("offset out of range")
)
