package mem

import (
	//"os"
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/cmd/kateway/inflights"
)

var _ inflights.Inflights = &memInflights{}

var msg = []byte("hello world")

func TestBasic(t *testing.T) {
	m := New("")
	err := m.TakeOff("cluster", "topic", "group", "partition", 1, msg)
	assert.Equal(t, nil, err)
	err = m.TakeOff("cluster", "topic", "group", "partition", 1, msg) // reentrant is ok
	assert.Equal(t, nil, err)
	assert.Equal(t, true, m.TakenOff("cluster", "topic", "group", "partition", 1))
	err = m.TakeOff("cluster", "topic", "group", "partition", 2, msg)
	assert.Equal(t, inflights.ErrOutOfOrder, err)
	var m1 []byte
	m1, err = m.LandX("cluster", "topic", "group", "partition", 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, "hello world", string(m1))
	err = m.Land("cluster", "topic", "group", "partition", 2)
	assert.Equal(t, inflights.ErrOutOfOrder, err)
	assert.Equal(t, false, m.TakenOff("cluster", "topic", "group", "partition", 1))
}

func TestInitAndStop(t *testing.T) {
	m := New("snapshot")
	assert.Equal(t, nil, m.Init())
	t.Logf("count: %d", m.offsets.Count())
	for it := range m.offsets.Iter() {
		t.Logf("%s %d %s", it.Key, it.Val.(message).Offset, string(it.Val.(message).Value))
	}
	m.TakeOff("cluster", "topic", "group", "partition0", 1, msg)
	m.TakeOff("cluster", "topic", "group", "partition1", 2, msg)
	assert.Equal(t, nil, m.Init())
	m.Stop()

	assert.Equal(t, nil, m.Init())
	assert.Equal(t, true, m.TakenOff("cluster", "topic", "group", "partition1", 2))
	assert.Equal(t, true, m.TakenOff("cluster", "topic", "group", "partition0", 1))
	assert.Equal(t, 2, m.offsets.Count())

	assert.Equal(t, false, m.TakenOff("cluster", "topic", "group", "partition3", 2))
	//os.Remove(m.snapshotFile)
}

func BenchmarkKey(b *testing.B) {
	b.ReportAllocs()
	m := New("")
	for i := 0; i < b.N; i++ {
		m.key("cluster", "topic", "group", "partition")
	}
}

func BenchmarkTakeOffThenLand(b *testing.B) {
	b.ReportAllocs()
	m := New("")
	for i := 0; i < b.N; i++ {
		m.TakeOff("cluster", "topic", "group", "partition", 1, msg)
		m.Land("cluster", "topic", "group", "partition", 1)
	}
}
