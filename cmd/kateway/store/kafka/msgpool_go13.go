// +build go1.3

package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
)

var (
	// producer message pool
	pmPool sync.Pool
)

func init() {
	pmPool.New = func() interface{} {
		return &sarama.ProducerMessage{}
	}
}

func pmGet() *sarama.ProducerMessage {
	return pmPool.Get().(*sarama.ProducerMessage)
}

func pmPut(m *sarama.ProducerMessage) {
	pmPool.Put(m)
}
