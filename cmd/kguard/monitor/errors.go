package monitor

import (
	"errors"
)

var (
	ErrClusterNotExist  = errors.New("cluster not exist")
	ErrBrokerIdNotExist = errors.New("broker id not exist")
	ErrTopicNotExist    = errors.New("topic not exist")
	ErrGroupNotExist    = errors.New("group not exist")
	ErrGroupNotOnTopic  = errors.New("group not on this topic")
)
