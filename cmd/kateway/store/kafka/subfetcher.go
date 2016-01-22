package kafka

import (
	"github.com/wvanbergen/kafka/consumergroup"
)

type consumerFetcher struct {
	*consumergroup.ConsumerGroup
	remoteAddr string
	store      *subStore
}

func (this *consumerFetcher) Close() {
	this.store.subPool.killClient(this.remoteAddr)
}
