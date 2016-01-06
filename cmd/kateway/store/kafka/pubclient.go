package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
)

type syncProducerClient struct {
	pool *pubPool

	id uint64
	sarama.SyncProducer
}

func (this *syncProducerClient) Close() {
	log.Trace("closing kafka sync client: %d", this.id)

	// will close the producer and the kafka tcp conn
	this.SyncProducer.Close()
}

func (this *syncProducerClient) Id() uint64 {
	return this.id
}

func (this *syncProducerClient) Recycle() {
	this.pool.syncPool.Put(this)
}

type asyncProducerClient struct {
	pool *pubPool

	id uint64
	sarama.AsyncProducer
}

func (this *asyncProducerClient) Close() {
	log.Trace("closing kafka async client: %d", this.id)

	// will flush any buffered message
	this.AsyncProducer.AsyncClose()
}

func (this *asyncProducerClient) Id() uint64 {
	return this.id
}

func (this *asyncProducerClient) Recycle() {
	this.pool.asyncPool.Put(this)
}
