package dumb

import (
	"github.com/Shopify/sarama"
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
