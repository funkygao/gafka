package manager

import (
	"errors"
)

var (
	ErrDisabledTopic      = errors.New("pub to a disabled topic not allowed")
	ErrEmptyIdentity      = errors.New("auth with empty identity")
	ErrAuthenticationFail = errors.New("authentication fails, retry after 5m")
	ErrAuthorizationFail  = errors.New("authorization fails, retry after 5m")
	ErrInvalidGroup       = errors.New("请登录web manager，在'我的App'的该App下注册该group")
	ErrSchemaNotFound     = errors.New("schema not found")
)
