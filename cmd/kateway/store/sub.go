package store

import (
	"github.com/Shopify/sarama"
)

type Fetcher interface {
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan *sarama.ConsumerError
	CommitUpto(*sarama.ConsumerMessage) error
}

type SubStore interface {
	Start() error

	KillClient(remoteAddr string)
	Fetch(cluster, topic, group, remoteAddr, reset string) (Fetcher, error)
}

var DefaultSubStore SubStore
