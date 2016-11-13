package zk

import (
	"github.com/funkygao/gafka/registry"
	"github.com/funkygao/gafka/zk"
	zklib "github.com/samuel/go-zookeeper/zk"
)

type dummy struct {
}

func New(*zk.ZkZone) registry.Backend {
	return &dummy{}
}

func (this *dummy) Name() string {
	return "dummy"
}

func (this *dummy) Register(id string, data []byte) {
	return
}

func (this *dummy) Deregister(id string, oldData []byte) error {
	return nil
}

func (this *dummy) WatchInstances() ([]string, <-chan zklib.Event, error) {
	return nil, nil, nil
}
