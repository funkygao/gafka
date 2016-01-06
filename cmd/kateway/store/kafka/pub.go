package kafka

import (
	l "log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
	ypool "github.com/youtube/vitess/go/pools"
)

type pubStore struct {
	shutdownCh chan struct{}

	maxRetries int
	wg         *sync.WaitGroup
	hostname   string
	dryRun     bool

	pubPools     map[string]*pubPool // key is cluster, each cluster maintains a conn pool
	poolsCapcity int
	poolsLock    sync.RWMutex
	idleTimeout  time.Duration

	// to avoid too frequent refresh
	// TODO refresh by cluster: current implementation will refresh zone
	lastRefreshedAt time.Time
}

func NewPubStore(poolCapcity int, maxRetries int, idleTimeout time.Duration,
	wg *sync.WaitGroup, debug bool, dryRun bool) *pubStore {
	if debug {
		sarama.Logger = l.New(os.Stdout, color.Green("[Sarama]"),
			l.LstdFlags|l.Lshortfile)
	}

	return &pubStore{
		hostname:     ctx.Hostname(),
		maxRetries:   maxRetries,
		idleTimeout:  idleTimeout,
		poolsCapcity: poolCapcity,
		pubPools:     make(map[string]*pubPool),
		wg:           wg,
		dryRun:       dryRun,
		shutdownCh:   make(chan struct{}),
	}
}

func (this *pubStore) Name() string {
	return "kafka"
}

func (this *pubStore) Start() (err error) {
	this.wg.Add(1)
	defer this.wg.Done()

	// warmup: create pools according the current kafka topology
	for _, cluster := range meta.Default.ClusterNames() {
		this.pubPools[cluster] = newPubPool(this,
			meta.Default.BrokerList(cluster), this.poolsCapcity)
	}

	go func() {
		// 5 seconds after meta store refresh
		time.Sleep(time.Second * 5)

		ticker := time.NewTicker(meta.Default.RefreshInterval())
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// FIXME the meta might not finished refresh yet
				// should use channel for synchronization
				this.doRefresh(false)

			case <-this.shutdownCh:
				log.Trace("kafka pub store stopped")
				return
			}
		}
	}()

	return
}

func (this *pubStore) Stop() {
	this.poolsLock.Lock()
	defer this.poolsLock.Unlock()

	// close all kafka connections
	for _, p := range this.pubPools {
		p.Close()
	}

	close(this.shutdownCh)
}

func (this *pubStore) doRefresh(force bool) {
	// TODO the lock is too big, should consider cluster level refresh
	this.poolsLock.Lock()
	defer this.poolsLock.Unlock()

	if time.Since(this.lastRefreshedAt) <= time.Second*5 {
		// too frequent refresh
		return
	}

	if force {
		meta.Default.Refresh()
		log.Trace("kafka clusters topology refreshed")
	}

	for _, cluster := range meta.Default.ClusterNames() {
		this.pubPools[cluster].RefreshBrokerList(meta.Default.BrokerList(cluster))
	}

	this.lastRefreshedAt = time.Now()
}

func (this *pubStore) SyncPub(cluster, topic string, key, msg []byte) (err error) {
	this.poolsLock.RLock()
	pool, present := this.pubPools[cluster]
	this.poolsLock.RUnlock()
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

	// TODO can be pooled
	producerMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	}

	if this.dryRun {
		producer, err = pool.GetSyncProducer()
		producer.Recycle()
		return
	}

	for i := 0; i < this.maxRetries; i++ {
		producer, err = pool.GetSyncProducer()
		if err != nil {
			// e,g the connection is idle too long, so when get, will triger
			// factory method, which might lead to kafka connection error

			if producer != nil {
				// should never happen
				log.Warn("cluster:%s, topic:%s pool fails while got a pub conn", cluster, topic)
				producer.Recycle()
			}

			switch err {
			case ypool.ErrClosed:
				// the connection pool is already closed
				return store.ErrShutdown

			case ypool.ErrTimeout:
				// the connection pool is too busy
				return store.ErrBusy

			case ErrorKafkaConfig, sarama.ErrOutOfBrokers:
				// the conn is idle too long which triggers the factory method and connect to kafka
				// all brokers down! retry for luck
				this.doRefresh(true)

			default:
				log.Warn("cluster:%s, topic:%s unknown pubPool err: %v", cluster, topic, err)
			}

			goto backoff
		}

		// TODO like java kafka lib, when pub fails, it should refresh metadata, and auto retry
		_, _, err = producer.SendMessage(producerMsg)
		producer.Recycle() // NEVER forget about this
		switch err {
		case nil:
			// send success
			return

		case sarama.ErrUnknownTopicOrPartition:
			log.Warn("cluster:%s topic:%s %v", cluster, topic, err)

		case breaker.ErrBreakerOpen:
			// will not retry
			return

		case sarama.ErrOutOfBrokers:
			producer.Close()
			this.doRefresh(true)

		default:
			producer.Close()
			log.Warn("cluster:%s topic:%s unknown pub err: %v", cluster, topic, err)
		}

	backoff:
		if retryDelay == 0 {
			retryDelay = 50 * time.Millisecond
		} else {
			retryDelay = 2 * retryDelay
		}
		if maxDelay := time.Second; retryDelay > maxDelay {
			retryDelay = maxDelay
		}

		log.Debug("cluster:%s topic:%s %v retry in %v", cluster, topic, err, retryDelay)
		time.Sleep(retryDelay)
	}

	return
}

// FIXME not fully fault tolerant like SyncPub.
func (this *pubStore) AsyncPub(cluster string, topic string, key []byte,
	msg []byte) (err error) {
	this.poolsLock.RLock()
	pool, present := this.pubPools[cluster]
	this.poolsLock.RUnlock()
	if !present {
		err = store.ErrInvalidCluster
		return
	}

	producer, e := pool.GetAsyncProducer()
	if e != nil {
		if producer != nil {
			producer.Recycle()
		}

		return e
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
