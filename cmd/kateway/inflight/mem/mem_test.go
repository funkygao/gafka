package mem

import (
	//"os"
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/cmd/kateway/inflight"
)

var _ inflight.Inflight = &memInflight{}

var msg = []byte("hello world")

func TestBasic(t *testing.T) {
	m := New("", true)
	err := m.TakeOff("cluster", "topic", "group", "partition", 1, msg)
	assert.Equal(t, nil, err)
	err = m.TakeOff("cluster", "topic", "group", "partition", 1, msg) // reentrant is ok
	assert.Equal(t, nil, err)
	err = m.TakeOff("cluster", "topic", "group", "partition", 2, msg)
	assert.Equal(t, inflight.ErrOutOfOrder, err)
	var m1 []byte
	m1, err = m.LandX("cluster", "topic", "group", "partition", 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, "hello world", string(m1))
	err = m.Land("cluster", "topic", "group", "partition", 2)
	assert.Equal(t, inflight.ErrOutOfOrder, err)
}

func TestInitAndStop(t *testing.T) {
	m := New("snapshot", true)
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
	assert.Equal(t, 2, m.offsets.Count())

	//os.Remove(m.snapshotFile)
}

func BenchmarkKey(b *testing.B) {
	b.ReportAllocs()
	m := New("", true)
	for i := 0; i < b.N; i++ {
		m.key("cluster", "topic", "group", "partition")
	}
}

func BenchmarkTakeOffThenLand(b *testing.B) {
	b.ReportAllocs()
	m := New("", true)
	for i := 0; i < b.N; i++ {
		m.TakeOff("cluster", "topic", "group", "partition", 1, msg)
		m.Land("cluster", "topic", "group", "partition", 1)
	}
}
