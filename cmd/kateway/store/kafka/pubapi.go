package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
)

func (this *pubStore) AddJob(cluster, topic string, payload []byte,
	delay time.Duration) (jobId string, err error) {
	this.jobPoolsLock.RLock()
	pool, present := this.jobPools[cluster]
	this.jobPoolsLock.RUnlock()
	if !present {
		err = store.ErrInvalidCluster
		return
	}

	return pool.AddJob(topic, payload, delay)
}

func (this *pubStore) DeleteJob(cluster, jobId string) error {
	this.jobPoolsLock.RLock()
	pool, present := this.jobPools[cluster]
	this.jobPoolsLock.RUnlock()
	if !present {
		return store.ErrInvalidCluster
	}

	return pool.DeleteJob(jobId)
}

func (this *pubStore) doSyncPub(allAck bool, cluster, topic string,
	key, msg []byte) (partition int32, offset int64, err error) {
	this.pubPoolsLock.RLock()
	pool, present := this.pubPools[cluster]
	this.pubPoolsLock.RUnlock()
	if !present {
		err = store.ErrInvalidCluster
		return
	}

	var (
		retryDelay time.Duration
		producer   *syncProducerClient
		keyEncoder sarama.Encoder = nil // will use random partitioner
	)
	if len(key) > 0 {
		keyEncoder = sarama.ByteEncoder(key) // will use hash partition
	}

	producerMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	}

	getProducer := pool.GetSyncProducer
	if allAck {
		getProducer = pool.GetSyncAllProducer
	}

	if this.dryRun {
		// ignore kafka I/O
		producer, err = getProducer()
		producer.Recycle()
		return
	}

	producer, err = getProducer()
	if err != nil {
		// e,g the connection is idle too long, so when get, will trigger
		// factory method, which might lead to kafka connection error
		if producer != nil {
			// should never happen
			producer.CloseAndRecycle()
		}

		return
	}

	for i := 0; i < this.maxRetries; i++ {
		// sarama will retry Producer.Retry.Max(3) times on produce failure before return error
		// meanwhile, it will auto refresh meta
		partition, offset, err = producer.SendMessage(producerMsg)
		if err == nil {
			// send ok
			producer.Recycle()
			return
		}

		log.Warn("cluster[%s] topic:%s %v", cluster, topic, err)
		switch err {
		case sarama.ErrUnknownTopicOrPartition:
			// will not retry
			producer.Recycle()
			return

		case breaker.ErrBreakerOpen, sarama.ErrOutOfBrokers:
			// will not retry
			producer.CloseAndRecycle()
			err = store.ErrBusy
			return

		default:
			// will retry
			producer.CloseAndRecycle()
		}

		if retryDelay == 0 {
			retryDelay = 50 * time.Millisecond
		} else {
			retryDelay = 2 * retryDelay
		}
		if maxDelay := time.Second; retryDelay > maxDelay {
			retryDelay = maxDelay
		}

		log.Warn("cluster[%s] topic:%s %v retry in %v", cluster, topic, err, retryDelay)
		time.Sleep(retryDelay)
	}

	return
}

func (this *pubStore) SyncAllPub(cluster, topic string, key, msg []byte) (partition int32, offset int64, err error) {
	return this.doSyncPub(true, cluster, topic, key, msg)
}

func (this *pubStore) SyncPub(cluster, topic string, key, msg []byte) (partition int32, offset int64, err error) {
	return this.doSyncPub(false, cluster, topic, key, msg)
}

// FIXME not fully fault tolerant like SyncPub.
func (this *pubStore) AsyncPub(cluster string, topic string, key []byte,
	msg []byte) (partition int32, offset int64, err error) {
	this.pubPoolsLock.RLock()
	pool, present := this.pubPools[cluster]
	this.pubPoolsLock.RUnlock()
	if !present {
		err = store.ErrInvalidCluster
		return
	}

	producer, e := pool.GetAsyncProducer()
	if e != nil {
		if producer != nil {
			producer.Recycle()
		}

		err = e
		return
	}

	var keyEncoder sarama.Encoder = nil // will use random partitioner
	if len(key) > 0 {
		keyEncoder = sarama.ByteEncoder(key) // will use hash partition
	}

	// TODO can be pooled
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	}
	producer.Recycle()
	return
}

func (this *pubStore) MarkPartitionsDead(topic string, deadPartitionIds map[int32]struct{}) {
	exclusivePartitionersLock.Lock()
	if deadPartitionIds == nil {
		// this topic comes alive
		delete(exclusivePartitioners, topic)
	} else {
		if p, present := exclusivePartitioners[topic]; present {
			p.markDead(deadPartitionIds)
		}
	}
	exclusivePartitionersLock.Unlock()
}
