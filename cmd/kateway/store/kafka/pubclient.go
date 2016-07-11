package kafka

import (
	"github.com/Shopify/sarama"
	pool "github.com/funkygao/golib/vitesspool"
	log "github.com/funkygao/log4go"
)

type syncProducerClient struct {
	rp *pool.ResourcePool

	cluster string
	id      uint64
	sarama.SyncProducer
	closed bool
}

func (this *syncProducerClient) Id() uint64 {
	return this.id
}

// Close must be called before Recycle
func (this *syncProducerClient) Close() {
	log.Debug("cluster[%s] closing kafka sync client: %d", this.cluster, this.id)

	// will close the producer and the kafka tcp conn
	this.SyncProducer.Close()
	this.closed = true
}

func (this *syncProducerClient) Recycle() {
	if this.closed {
		this.rp.Put(nil)
	} else {
		this.rp.Put(this)
	}
}

func (this *syncProducerClient) CloseAndRecycle() {
	this.Close()
	this.Recycle()
}

type asyncProducerClient struct {
	rp *pool.ResourcePool

	cluster string
	id      uint64
	sarama.AsyncProducer
}

func (this *asyncProducerClient) Close() {
	log.Debug("cluster[%s] closing kafka async client: %d", this.cluster, this.id)

	// will flush any buffered message
	this.AsyncProducer.AsyncClose()
}

func (this *asyncProducerClient) Id() uint64 {
	return this.id
}

func (this *asyncProducerClient) Recycle() {
	this.rp.Put(this)
}
