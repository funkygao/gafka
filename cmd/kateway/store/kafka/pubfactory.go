package kafka

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
	pool "github.com/youtube/vitess/go/pools"
)

func (this *pubPool) newSyncProducer(requiredAcks sarama.RequiredAcks) (pool.Resource, error) {
	if len(this.brokerList) == 0 {
		return nil, store.ErrEmptyBrokers
	}

	spc := &syncProducerClient{
		cluster: this.cluster,
		id:      atomic.AddUint64(&this.nextId, 1),
	}
	switch requiredAcks {
	case sarama.WaitForAll:
		spc.rp = this.syncAllPool

	case sarama.WaitForLocal:
		spc.rp = this.syncPool

	default:
		return nil, errors.New("illegal ack type")
	}

	var err error
	t1 := time.Now()
	cf := sarama.NewConfig()
	cf.Net.DialTimeout = time.Second * 10
	cf.Net.ReadTimeout = time.Second * 10
	cf.Net.WriteTimeout = time.Second * 10

	cf.Metadata.RefreshFrequency = time.Minute * 10
	cf.Metadata.Retry.Max = 3
	cf.Metadata.Retry.Backoff = time.Millisecond * 200

	cf.Producer.Timeout = time.Second * 1
	cf.Producer.RequiredAcks = requiredAcks
	cf.Producer.Partitioner = NewExclusivePartitioner
	cf.Producer.Return.Successes = false
	cf.Producer.Retry.Backoff = time.Millisecond * 200
	cf.Producer.Retry.Max = 3
	if this.store.compress {
		cf.Producer.Compression = sarama.CompressionSnappy
	}

	cf.ClientID = this.store.hostname

	cf.ChannelBufferSize = 256 // TODO

	// will fetch meta from broker list
	spc.SyncProducer, err = sarama.NewSyncProducer(this.brokerList, cf)
	if err != nil {
		return nil, err
	}

	log.Debug("cluster[%s] kafka connected[%d]: %+v %s",
		this.cluster, spc.id, this.brokerList, time.Since(t1))

	return spc, err
}

func (this *pubPool) syncAllProducerFactory() (pool.Resource, error) {
	return this.newSyncProducer(sarama.WaitForAll)
}

func (this *pubPool) syncProducerFactory() (pool.Resource, error) {
	return this.newSyncProducer(sarama.WaitForLocal)
}

func (this *pubPool) asyncProducerFactory() (pool.Resource, error) {
	if len(this.brokerList) == 0 {
		return nil, store.ErrEmptyBrokers
	}

	apc := &asyncProducerClient{
		rp:      this.asyncPool,
		cluster: this.cluster,
		id:      atomic.AddUint64(&this.nextId, 1),
	}

	var err error
	t1 := time.Now()
	cf := sarama.NewConfig()
	cf.Metadata.RefreshFrequency = time.Minute * 10
	cf.Metadata.Retry.Max = 3

	cf.Producer.Flush.Frequency = time.Second * 10 // TODO
	cf.Producer.Flush.Messages = 1000
	cf.Producer.Flush.MaxMessages = 0 // unlimited

	cf.Producer.RequiredAcks = sarama.NoResponse
	cf.Producer.Partitioner = NewExclusivePartitioner
	cf.Producer.Retry.Backoff = time.Millisecond * 200
	cf.Producer.Retry.Max = 3
	if this.store.compress {
		cf.Producer.Compression = sarama.CompressionSnappy
	}

	cf.ClientID = this.store.hostname

	apc.AsyncProducer, err = sarama.NewAsyncProducer(this.brokerList, cf)
	if err != nil {
		return nil, err
	}

	log.Debug("cluster[%s] kafka connected[%d]: %+v %s",
		this.cluster, apc.id, this.brokerList, time.Since(t1))

	// TODO
	go func() {
		// messages will only be returned here after all retry attempts are exhausted.
		for err := range apc.Errors() {
			log.Error("cluster[%s] async producer: %v", this.cluster, err)
		}
	}()

	return apc, err
}
