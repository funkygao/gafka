package mem

import (
	"fmt"

	"github.com/funkygao/gafka/cmd/kateway/inflights"
	"github.com/funkygao/golib/cmap"
)

type memInflights struct {
	offsets cmap.ConcurrentMap
}

func New() *memInflights {
	return &memInflights{
		offsets: cmap.New(),
	}
}

func (this *memInflights) key(cluster, topic, group, partition string) string {
	// FIXME bad perf
	return fmt.Sprintf("%s:%s:%s:%s", cluster, topic, group, partition)
}

func (this *memInflights) Land(cluster, topic, group, partition string, offset int64) error {
	key := this.key(cluster, topic, group, partition)
	o, found := this.offsets.Get(key)
	if !found || o.(int64) != offset {
		return inflights.ErrOutOfOrder
	}

	this.offsets.Remove(key)
	return nil
}

// FIXME not atomic, add CAS
func (this *memInflights) TakeOff(cluster, topic, group, partition string, offset int64) error {
	key := this.key(cluster, topic, group, partition)
	o, found := this.offsets.Get(key)
	if found && o.(int64) != offset {
		return inflights.ErrOutOfOrder
	}

	this.offsets.Set(key, offset)
	return nil
}
