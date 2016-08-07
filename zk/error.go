package zk

import (
	"errors"
)

var (
	ErrDupConnect      = errors.New("connect while being connected")
	ErrClaimedByOthers = errors.New("claimed by others")
	ErrNotClaimed      = errors.New("release non-claimed")
)
