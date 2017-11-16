package monitor

import (
	"errors"
)

var (
	ErrClusterNotExist  = errors.New("cluster not exist")
	ErrBrokerIdNotExist = errors.New("broker id not exist")
	ErrTopicNotExist    = errors.New("topic not exist")
)
