package kafka

import (
	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
)

type syncProducerClient struct {
	pool *pubPool

	id uint64
	sarama.SyncProducer
	closed bool
}

func (this *syncProducerClient) Id() uint64 {
	return this.id
}

// Close must be called before Recycle
func (this *syncProducerClient) Close() {
	log.Trace("cluster[%s] closing kafka sync client: %d", this.pool.cluster, this.id)

	// will close the producer and the kafka tcp conn
	this.SyncProducer.Close()
	this.closed = true
}

func (this *syncProducerClient) Recycle() {
	if this.closed {
		this.pool.syncPool.Put(nil)
	} else {
		this.pool.syncPool.Put(this)
	}
}

func (this *syncProducerClient) CloseAndRecycle() {
	this.Close()
	this.Recycle()
}

type asyncProducerClient struct {
	pool *pubPool

	id uint64
	sarama.AsyncProducer
}

func (this *asyncProducerClient) Close() {
	log.Trace("cluster[%s] closing kafka async client: %d", this.pool.cluster, this.id)

	// will flush any buffered message
	this.AsyncProducer.AsyncClose()
}

func (this *asyncProducerClient) Id() uint64 {
	return this.id
}

func (this *asyncProducerClient) Recycle() {
	this.pool.asyncPool.Put(this)
}
