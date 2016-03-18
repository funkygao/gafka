package mem

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/funkygao/gafka/cmd/kateway/inflights"
	"github.com/funkygao/golib/cmap"
	"github.com/funkygao/golib/io"
)

type record struct {
	Key string
	Val int64
}

type memInflights struct {
	offsets cmap.ConcurrentMap

	snapshotFile string
}

func New(fn string) *memInflights {
	return &memInflights{
		offsets:      cmap.New(),
		snapshotFile: fn,
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

func (this *memInflights) TakenOff(cluster, topic, group, partition string, offset int64) bool {
	key := this.key(cluster, topic, group, partition)
	o, found := this.offsets.Get(key)
	if found && o.(int64) == offset {
		return true
	}

	return false
}

func (this *memInflights) Init() error {
	if this.snapshotFile == "" {
		return nil
	}

	if !io.DirExists(this.snapshotFile) {
		return nil
	}

	data, err := ioutil.ReadFile(this.snapshotFile)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &this.offsets)
}

func (this *memInflights) Stop() error {
	if this.snapshotFile == "" {
		return nil
	}

	dumps := make([]record, 0, this.offsets.Count())
	for item := range this.offsets.Iter() {
		dumps = append(dumps, record{
			Key: item.Key,
			Val: item.Val.(int64),
		})
	}
	data, err := json.Marshal(dumps)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(this.snapshotFile, data, 0644)
}
