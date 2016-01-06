// +build !go1.3

package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
)

func pmGet() *sarama.ProducerMessage {
	return &sarama.ProducerMessage{}
}

func pmPut(m *sarama.ProducerMessage) {}
