package store

import (
	"github.com/Shopify/sarama"
)

// A Fetcher is a generic high level streamed consumer.
type Fetcher interface {
	// Messages returns a stream messages being consumed.
	Messages() <-chan *sarama.ConsumerMessage

	// Errors returns a stream errors during consuming.
	Errors() <-chan *sarama.ConsumerError

	// CommitUpto records the cursor/offset of where messages are consumed.
	CommitUpto(*sarama.ConsumerMessage) error

	// Close the Fetcher and do all the cleanups.
	Close() error
}

// A SubStore is a generic data source that can be used to fetch messages.
type SubStore interface {
	// Name returns the name of the underlying store.
	Name() string

	Start() error
	Stop()

	// Fetch returns a Fetcher.
	Fetch(cluster, topic, group, remoteAddr, realIp, resetOffset string, permitStandby bool) (Fetcher, error)

	IsSystemError(error) bool
}

var DefaultSubStore SubStore
