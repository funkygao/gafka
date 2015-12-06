package dumb

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
)

type consumerFetcher struct {
	ch chan *sarama.ConsumerMessage
}

func (this *consumerFetcher) Messages() <-chan *sarama.ConsumerMessage {
	return this.ch
}

func (this *consumerFetcher) Errors() <-chan *sarama.ConsumerError {
	return nil
}

func (this *consumerFetcher) CommitUpto(*sarama.ConsumerMessage) error {
	return nil
}

type subStore struct {
	fetcher *consumerFetcher
}

func NewSubStore(meta meta.MetaStore, wg *sync.WaitGroup,
	shutdownCh <-chan struct{}, closedConnCh <-chan string, debug bool) *subStore {
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
		Value: []byte("hello from dumb fetcher"),
	}

	go func() {
		for {
			this.fetcher.ch <- msg
		}
	}()

	return
}

func (this *subStore) KillClient(remoteAddr string) {

}
func (this *subStore) Fetch(cluster, topic, group, remoteAddr, reset string) (store.Fetcher, error) {
	return this.fetcher, nil
}
