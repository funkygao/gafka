package main

import (
	"errors"
)

var (
	ErrClientGone       = errors.New("remote client gone")
	ErrTooBigPubMessage = errors.New("too big message")
)
