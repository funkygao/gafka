package store

import (
	"github.com/Shopify/sarama"
)

// A Fetcher is a generic streamed consumer.
type Fetcher interface {
	// Messages returns a stream messages being consumed.
	Messages() <-chan *sarama.ConsumerMessage

	// Errors returns a stream errors during consuming.
	Errors() <-chan *sarama.ConsumerError

	// CommitUpto records the cursor/offset of where messages are consumed.
	CommitUpto(*sarama.ConsumerMessage) error
}

// A SubStore is a generic data source that can be used to fetch messages.
type SubStore interface {
	// Name returns the name of the underlying store.
	Name() string

	Start() error
	Stop()

	KillClient(remoteAddr string)

	// Fetch returns a Fetcher.
	Fetch(cluster, topic, group, remoteAddr, reset string) (Fetcher, error)
}

var DefaultSubStore SubStore
