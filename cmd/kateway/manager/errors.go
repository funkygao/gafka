package manager

import (
	"errors"
)

var (
	ErrEmptyIdentity      = errors.New("auth with empty identity")
	ErrAuthenticationFail = errors.New("authentication fails, retry after 5m")
	ErrAuthorizationFail  = errors.New("authorization fails, retry after 5m")
	ErrInvalidGroup       = errors.New("请登录web manager，在'我的App'的该App下注册该group，注册成功后5分钟后生效")
)
