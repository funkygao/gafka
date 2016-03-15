package mem

import (
	"testing"

	"github.com/funkygao/assert"
	"github.com/funkygao/gafka/cmd/kateway/inflights"
)

var _ inflights.Inflights = &memInflights{}

func TestBasic(t *testing.T) {
	m := New()
	err := m.TakeOff("cluster", "topic", "group", "partition", 1)
	assert.Equal(t, nil, err)
	err = m.TakeOff("cluster", "topic", "group", "partition", 1) // reentrant is ok
	assert.Equal(t, nil, err)
	err = m.TakeOff("cluster", "topic", "group", "partition", 2)
	assert.Equal(t, inflights.ErrOutOfOrder, err)
	err = m.Land("cluster", "topic", "group", "partition", 1)
	assert.Equal(t, nil, err)
	err = m.Land("cluster", "topic", "group", "partition", 2)
	assert.Equal(t, inflights.ErrOutOfOrder, err)
}

func BenchmarkKey(b *testing.B) {
	b.ReportAllocs()
	m := New()
	for i := 0; i < b.N; i++ {
		m.key("cluster", "topic", "group", "partition")
	}
}

func BenchmarkTakeOffThenLand(b *testing.B) {
	b.ReportAllocs()
	m := New()
	for i := 0; i < b.N; i++ {
		m.TakeOff("cluster", "topic", "group", "partition", 1)
		m.Land("cluster", "topic", "group", "partition", 1)
	}
}
