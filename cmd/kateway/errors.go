package main

import (
	"errors"
)

var (
	ErrRemoteInterrupt  = errors.New("remote client interrupted")
	ErrTooBigPubMessage = errors.New("too big message")
)
