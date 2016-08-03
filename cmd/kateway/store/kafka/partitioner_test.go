package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/funkygao/assert"
)

func TestExclusivePartitioner(t *testing.T) {
	p := NewExclusivePartitioner("topic").(*exclusivePartitioner)
	msg := &sarama.ProducerMessage{
		Topic: "foo",
		Key:   sarama.StringEncoder("bar1xxwz"),
	}

	partitionsN := int32(4)

	partitionId, err := p.Partition(msg, partitionsN)
	assert.Equal(t, int32(0), partitionId)
	assert.Equal(t, nil, err)

	msg.Key = sarama.StringEncoder("asdf")
	partitionId, _ = p.Partition(msg, partitionsN)
	assert.Equal(t, int32(3), partitionId)

	ps := &pubStore{}
	ps.markPartitionsDead("foo", map[int32]struct{}{
		3: struct{}{},
	})
	partitionId, _ = p.Partition(msg, partitionsN)
	assert.Equal(t, int32(0), partitionId) // rounded to 0
}

// 40 ns/op
func BenchmarkPartitionerEmptyKey(b *testing.B) {
	msg := &sarama.ProducerMessage{
		Topic: "foo",
		Key:   nil,
	}
	p := NewExclusivePartitioner("foo")
	for i := 0; i < b.N; i++ {
		p.Partition(msg, 5)
	}
}

// 100 ns/op
func BenchmarkPartitionerNonEmptyKey(b *testing.B) {
	msg := &sarama.ProducerMessage{
		Topic: "foo",
		Key:   sarama.StringEncoder("bar"),
	}
	p := NewExclusivePartitioner("foo")
	for i := 0; i < b.N; i++ {
		p.Partition(msg, 5)
	}
}
