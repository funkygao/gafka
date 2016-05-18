package gateway

import (
	"errors"
)

var (
	ErrClientGone           = errors.New("remote client gone")
	ErrTooBigMessage        = errors.New("too big message")
	ErrTooSmallMessage      = errors.New("too small message")
	ErrIllegalTaggedMessage = errors.New("illegal tagged message")
)
