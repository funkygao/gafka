package kafka

import (
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	pool "github.com/youtube/vitess/go/pools"
)

func (this *pubPool) asyncProducerFactory() (pool.Resource, error) {
	apc := &asyncProducerClient{
		pool: this,
		id:   atomic.AddUint64(&this.nextId, 1),
	}

	var err error
	t1 := time.Now()
	cf := sarama.NewConfig()
	cf.Metadata.RefreshFrequency = time.Minute // TODO
	cf.Metadata.Retry.Max = 3
	cf.Producer.Flush.Frequency = time.Second * 10
	cf.Producer.Flush.Messages = 1000
	cf.Producer.Flush.MaxMessages = 1000
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second * 4
	cf.ClientID = this.store.hostname
	cf.Producer.Retry.Max = 3
	//cf.Producer.Compression = sarama.CompressionSnappy TODO

	apc.AsyncProducer, err = sarama.NewAsyncProducer(this.brokerList, cf)
	if err != nil {
		return nil, err
	}

	log.Trace("kafka connected[%d]: %+v %s", apc.id, this.brokerList,
		time.Since(t1))

	// TODO
	go func() {
		// messages will only be returned here after all retry attempts are exhausted.
		for err := range apc.Errors() {
			log.Error("async producer: %v", err)
		}
	}()

	return apc, err
}

func (this *pubPool) syncProducerFactory() (pool.Resource, error) {
	spc := &syncProducerClient{
		pool: this,
		id:   atomic.AddUint64(&this.nextId, 1),
	}

	var err error
	t1 := time.Now()
	cf := sarama.NewConfig()
	cf.Metadata.RefreshFrequency = time.Minute // TODO
	cf.Metadata.Retry.Max = 3
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second * 4
	cf.ClientID = this.store.hostname
	cf.Producer.Retry.Max = 3
	//cf.Producer.Compression = sarama.CompressionSnappy

	spc.SyncProducer, err = sarama.NewSyncProducer(this.brokerList, cf)
	if err != nil {
		return nil, err
	}

	log.Trace("kafka connected[%d]: %+v %s", spc.id, this.brokerList,
		time.Since(t1))

	return spc, err
}
