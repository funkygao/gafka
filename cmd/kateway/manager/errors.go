package manager

import (
	"errors"
)

var (
	ErrDisabledTopic      = errors.New("pub to a disabled topic not allowed")
	ErrEmptyIdentity      = errors.New("auth with empty identity or key")
	ErrAuthenticationFail = errors.New("authentication fails")
	ErrAuthorizationFail  = errors.New("authorization fails")
	ErrInvalidGroup       = errors.New("group must be registered before usage")
	ErrSchemaNotFound     = errors.New("schema not found")
)
