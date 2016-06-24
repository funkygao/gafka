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

	if pool.breaker.Open() {
		err = store.ErrCircuitOpen
		return
	}

	var (
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
		// e,g. during factory method, kafka breaks down
		pool.breaker.Fail()

		if producer != nil {
			// should never happen
			producer.CloseAndRecycle()
		}

		return
	}

	// sarama will retry Producer.Retry.Max(3) times on produce failure before return error
	// meanwhile, it will auto refresh meta
	partition, offset, err = producer.SendMessage(producerMsg)
	if err == nil {
		// send ok
		pool.breaker.Succeed()
		producer.Recycle()
		return
	}

	log.Warn("cluster[%s] topic:%s %v", cluster, topic, err)
	switch err {
	// read tcp 10.209.36.33:50607->10.209.18.16:11005: i/o timeout
	// dial tcp 10.209.18.65:11005: getsockopt: connection refused

	case sarama.ErrUnknownTopicOrPartition:
		// this conn is still valid
		pool.breaker.Succeed()
		producer.Recycle()
		return

	case breaker.ErrBreakerOpen, sarama.ErrOutOfBrokers:
		// sarama is using breaker: 3 error/1 success/10s
		// will not retry FIXME breaker didn't work
		pool.breaker.Fail()
		producer.CloseAndRecycle()
		// err = store.ErrBusy TODO hide the underlying err
		return

	default:
		// e,g. sarama.ErrLeaderNotAvailable, sarama.ErrNotLeaderForPartition
		// will retry
		pool.breaker.Fail()
		producer.CloseAndRecycle()
		// err = store.ErrBusy TODO hide the underlying err
	}

	return
}

func (this *pubStore) IsSystemError(err error) bool {
	switch err {
	case store.ErrInvalidCluster:
		return false

	default:
		return true
	}
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
