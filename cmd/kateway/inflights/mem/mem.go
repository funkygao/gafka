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

func (this *memInflights) key(topic, ver, group, partition string) string {
	// FIXME bad perf
	return fmt.Sprintf("%s:%s:%s:%s", topic, ver, group, partition)
}

func (this *memInflights) Land(topic, ver, group, partition string) {
	this.offsets.Remove(this.key(topic, ver, group, partition))
}

func (this *memInflights) TakeOff(topic, ver, group, partition string, offset int64) error {
	o, found := this.offsets.Get(this.key(topic, ver, group, partition))
	if found && o.(int64) != offset {
		return inflights.ErrOutOfOrder
	}

	this.offsets.Set(this.key(topic, ver, group, partition), offset)
	return nil
}
