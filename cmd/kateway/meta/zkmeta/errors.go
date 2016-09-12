package zkmeta

import (
	"errors"
)

var (
	ErrZkBroken = errors.New("zk connection might be broken cause no clusters found")
)
