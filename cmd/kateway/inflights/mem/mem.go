package mem

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/funkygao/gafka/cmd/kateway/inflights"
	"github.com/funkygao/golib/cmap"
)

type dumpRecord struct {
	Key string
	Val message
}

type message struct {
	Offset int64
	Value  []byte
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
	if !found || o.(message).Offset != offset {
		return inflights.ErrOutOfOrder
	}

	this.offsets.Remove(key)
	return nil
}

func (this *memInflights) LandX(cluster, topic, group, partition string, offset int64) ([]byte, error) {
	key := this.key(cluster, topic, group, partition)
	o, found := this.offsets.Get(key)
	if !found || o.(message).Offset != offset {
		return nil, inflights.ErrOutOfOrder
	}

	msg := o.(message).Value

	this.offsets.Remove(key)
	return msg, nil
}

// FIXME not atomic, add CAS
func (this *memInflights) TakeOff(cluster, topic, group, partition string, offset int64, msg []byte) error {
	key := this.key(cluster, topic, group, partition)
	o, found := this.offsets.Get(key)
	if found && o.(message).Offset != offset {
		return inflights.ErrOutOfOrder
	}

	this.offsets.Set(key, message{
		Offset: offset,
		Value:  msg,
	})
	return nil
}

func (this *memInflights) TakenOff(cluster, topic, group, partition string, offset int64) bool {
	key := this.key(cluster, topic, group, partition)
	o, found := this.offsets.Get(key)
	if found && o.(message).Offset == offset {
		return true
	}

	return false
}

func (this *memInflights) Init() error {
	if this.snapshotFile == "" {
		return nil
	}

	data, err := ioutil.ReadFile(this.snapshotFile)
	if err != nil {
		return err
	}

	dumps := make([]dumpRecord, 0)
	if err = json.Unmarshal(data, &dumps); err != nil {
		return err
	}
	for _, record := range dumps {
		this.offsets.Set(record.Key, record.Val)
	}
	return nil
}

func (this *memInflights) Stop() error {
	if this.snapshotFile == "" {
		return nil
	}

	dumps := make([]dumpRecord, 0, this.offsets.Count())
	for item := range this.offsets.Iter() {
		dumps = append(dumps, dumpRecord{
			Key: item.Key,
			Val: item.Val.(message),
		})
	}
	data, err := json.Marshal(dumps)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(this.snapshotFile, data, 0644)
}
