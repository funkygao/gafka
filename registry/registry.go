package registry

import (
	"github.com/samuel/go-zookeeper/zk"
)

type Backend interface {
	Registered(id string) (bool, error)

	Register(id string, data []byte) error

	Deregister(id string, data []byte) error

	WatchInstances() ([]string, <-chan zk.Event, error)

	// Name of the registry backend.
	Name() string
}

var Default Backend
