package dummy

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/store"
)

type subStore struct {
	fetcher *consumerFetcher
}

func NewSubStore(wg *sync.WaitGroup, closedConnCh <-chan string, debug bool) *subStore {
	return &subStore{
		fetcher: &consumerFetcher{
			ch: make(chan *sarama.ConsumerMessage, 1000),
		},
	}
}

func (this *subStore) Start() (err error) {
	msg := &sarama.ConsumerMessage{
		Topic: "hello",
		Key:   []byte("world"),
		Value: []byte("hello from dummy fetcher"),
	}

	go func() {
		for {
			this.fetcher.ch <- msg
		}
	}()

	return
}

func (this *subStore) Stop() {}

func (this *subStore) Name() string {
	return "dummy"
}

func (this *subStore) IsSystemError(error) bool {
	return false
}

func (this *subStore) Fetch(cluster, topic, group, remoteAddr, realIp,
	reset string, permitStandby bool) (store.Fetcher, error) {
	return this.fetcher, nil
}
