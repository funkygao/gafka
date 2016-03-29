package mem

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/funkygao/gafka/cmd/kateway/inflight"
	"github.com/funkygao/golib/cmap"
	log "github.com/funkygao/log4go"
)

type dumpRecord struct {
	Key string
	Val message
}

type message struct {
	Offset int64
	Value  []byte
}

type memInflight struct {
	offsets cmap.ConcurrentMap

	snapshotFile string
	debug        bool
}

func New(fn string, debug bool) *memInflight {
	return &memInflight{
		offsets:      cmap.New(),
		snapshotFile: fn,
		debug:        debug,
	}
}

// FIXME bad perf
func (this *memInflight) key(cluster, topic, group, partition string) string {
	return fmt.Sprintf("%s:%s:%s:%s", cluster, topic, group, partition)
}

func (this *memInflight) Land(cluster, topic, group, partition string, offset int64) error {
	key := this.key(cluster, topic, group, partition)
	if this.debug {
		log.Debug("Land %s => %d", key, offset)
	}
	o, found := this.offsets.Get(key)
	if !found || o.(message).Offset != offset {
		if this.debug {
			log.Error("out of order: %s", this)
		}
		return inflight.ErrOutOfOrder
	}

	this.offsets.Remove(key)
	return nil
}

func (this *memInflight) LandX(cluster, topic, group, partition string, offset int64) ([]byte, error) {
	key := this.key(cluster, topic, group, partition)
	if this.debug {
		log.Debug("LandX %s => %d", key, offset)
	}
	o, found := this.offsets.Get(key)
	if !found || o.(message).Offset != offset {
		if this.debug {
			log.Error("out of order: %s", this)
		}
		return nil, inflight.ErrOutOfOrder
	}

	msg := o.(message).Value

	this.offsets.Remove(key)
	return msg, nil
}

// FIXME not atomic, add CAS
func (this *memInflight) TakeOff(cluster, topic, group, partition string, offset int64, msg []byte) error {
	key := this.key(cluster, topic, group, partition)
	if this.debug {
		log.Debug("TakeOff %s => %d", key, offset)
	}
	o, found := this.offsets.Get(key)
	if found && o.(message).Offset != offset {
		if this.debug {
			log.Error("out of order: %s", this)
		}
		return inflight.ErrOutOfOrder
	}

	this.offsets.Set(key, message{
		Offset: offset,
		Value:  msg,
	})
	return nil
}

func (this *memInflight) String() string {
	dumps := make([]dumpRecord, 0, this.offsets.Count())
	for item := range this.offsets.Iter() {
		dumps = append(dumps, dumpRecord{
			Key: item.Key,
			Val: item.Val.(message),
		})
	}
	data, _ := json.Marshal(dumps)
	return string(data)
}

func (this *memInflight) Init() error {
	if this.snapshotFile == "" {
		return nil
	}

	data, err := ioutil.ReadFile(this.snapshotFile)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil
		}
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

func (this *memInflight) Stop() error {
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
