package kafka

import (
	"github.com/funkygao/kafka-cg/consumergroup"
)

type consumerFetcher struct {
	*consumergroup.ConsumerGroup
	remoteAddr string
	store      *subStore
}

func (this *consumerFetcher) Close() {
	this.store.subPool.killClient(this.remoteAddr)
}
