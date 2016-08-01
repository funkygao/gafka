package kafka

import (
	"github.com/funkygao/kafka-cg/consumergroup"
)

type consumerFetcher struct {
	*consumergroup.ConsumerGroup
	remoteAddr string
	store      *subStore
}

func (this *consumerFetcher) Close() error {
	return this.store.subManager.killClient(this.remoteAddr)
}
