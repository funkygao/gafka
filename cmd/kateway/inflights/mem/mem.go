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

func (this *memInflights) Land(cluster, topic, group, partition string) {
	this.offsets.Remove(this.key(cluster, topic, group, partition))
}

// FIXME not atomic, add CAS
func (this *memInflights) TakeOff(cluster, topic, group, partition string, offset int64) error {
	o, found := this.offsets.Get(this.key(cluster, topic, group, partition))
	if found && o.(int64) != offset {
		return inflights.ErrOutOfOrder
	}

	this.offsets.Set(this.key(cluster, topic, group, partition), offset)
	return nil
}
