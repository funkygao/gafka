package manager

import (
	"errors"
)

var (
	ErrEmptyIdentity      = errors.New("auth with empty identity")
	ErrPermDenied         = errors.New("permission denied")
	ErrAuthenticationFail = errors.New("authentication fails, retry after 5m")
	ErrAuthorizationFial  = errors.New("authorization fails, retry after 5m")
)
