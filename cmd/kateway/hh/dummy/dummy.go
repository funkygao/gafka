package dummy

import (
	"github.com/funkygao/gafka/cmd/kateway/hh"
)

type dummyStore struct {
}

func New() hh.Service {
	return &dummyStore{}
}

func (this *dummyStore) Start() (err error) {
	return
}

func (this *dummyStore) Stop() {}

func (this *dummyStore) Name() string {
	return "dummy"
}

func (this *dummyStore) Append(cluster, topic string, key, value []byte) error {
	return nil
}

func (this *dummyStore) Empty(cluster, topic string) bool {
	return true
}

func (this *dummyStore) FlushInflights() {}

func (this *dummyStore) Inflights() int64 {
	return 0
}

func (this *dummyStore) AppendN() int64 {
	return 0
}

func (this *dummyStore) DeliverN() int64 {
	return 0
}
