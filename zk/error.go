package zk

import (
	"errors"
)

var (
	ErrDupConnect = errors.New("connect while being connected")
)
