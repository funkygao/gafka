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
)

type pubStore struct {
	shutdownCh chan struct{}

	maxRetries int
	wg         *sync.WaitGroup
	hostname   string // used as kafka client id
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
		this.pubPools[cluster] = newPubPool(this, cluster,
			meta.Default.BrokerList(cluster), this.poolsCapcity)
	}

	go func() {
		for {
			select {
			case <-meta.Default.RefreshEvent():
				this.doRefresh()

			case <-this.shutdownCh:
				log.Trace("pub store[%s] stopped", this.Name())
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
	for _, pool := range this.pubPools {
		pool.Close()
	}

	close(this.shutdownCh)
}

func (this *pubStore) doRefresh() {
	// TODO the lock is too big, should consider cluster level refresh
	this.poolsLock.Lock()
	defer this.poolsLock.Unlock()

	if time.Since(this.lastRefreshedAt) <= time.Second*5 {
		log.Warn("ignored too frequent refresh: %s", time.Since(this.lastRefreshedAt))
		return
	}

	activeClusters := make(map[string]struct{})
	for _, cluster := range meta.Default.ClusterNames() {
		activeClusters[cluster] = struct{}{}
		if _, present := this.pubPools[cluster]; !present {
			this.pubPools[cluster] = newPubPool(this, cluster,
				meta.Default.BrokerList(cluster), this.poolsCapcity)
		} else {
			this.pubPools[cluster].RefreshBrokerList(meta.Default.BrokerList(cluster))
		}
	}

	// shutdown the dead clusters
	for cluster, pool := range this.pubPools {
		if _, present := activeClusters[cluster]; !present {
			// this cluster is dead or removed forever
			pool.Close()
			delete(this.pubPools, cluster)
		}
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

	// TODO can be pooled, see msgpool
	producerMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	}

	if this.dryRun {
		// ignore kafka I/O
		producer, err = pool.GetSyncProducer()
		producer.Recycle()
		return
	}

	producer, err = pool.GetSyncProducer()
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
		_, _, err = producer.SendMessage(producerMsg)
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
			return store.ErrBusy

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
