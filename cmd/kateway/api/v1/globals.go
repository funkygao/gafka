package api

import (
	"errors"
)

var (
	ErrSubStop     = errors.New("sub stopped")
	ErrInvalidBury = errors.New("invalid bury name")
)

const (
	ShadowRetry = "retry"
	ShadowDead  = "dead"

	UserAgent = "pubsub-go v0.1"
)
