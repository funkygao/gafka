package main

import (
	"errors"
)

var (
	ErrClientGone           = errors.New("remote client gone")
	ErrTooBigPubMessage     = errors.New("too big message")
	ErrIllegalTaggedMessage = errors.New("illegal tagged message")
	ErrTooSmallPubMessage   = errors.New("too small message")
)
