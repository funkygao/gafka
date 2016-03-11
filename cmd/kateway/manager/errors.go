package manager

import (
	"errors"
)

var (
	ErrEmptyParam         = errors.New("auth with empty params")
	ErrPermDenied         = errors.New("permission denied")
	ErrAuthenticationFail = errors.New("authentication fails")
	ErrAuthorizationFial  = errors.New("authorization fails")
)
