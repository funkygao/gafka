package zk

import (
	"github.com/funkygao/gafka/registry"
	"github.com/funkygao/gafka/zk"
	zklib "github.com/samuel/go-zookeeper/zk"
)

// eureka is a service discovery implementation that uses netflix eureka
// as backend, which is AP system.
//
// register, renew, cancel, get is all eureka provides.
type eureka struct {
}

func New(*zk.ZkZone) registry.Backend {
	return &eureka{}
}

func (this *eureka) Name() string {
	return "eureka"
}

func (this *eureka) Register(id string, data []byte) {
	return
}

func (this *eureka) Deregister(id string, oldData []byte) error {
	return nil
}

func (this *eureka) WatchInstances() ([]string, <-chan zklib.Event, error) {
	return nil, nil, nil
}
