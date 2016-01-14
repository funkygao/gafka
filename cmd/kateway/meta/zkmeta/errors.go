package zkmeta

import (
	"errors"
)

var (
	ErrEmptyParam         = errors.New("empty params")
	ErrAuthenticationFail = errors.New("authentication fails")
	ErrAuthorizationFial  = errors.New("authorization fails")
)
