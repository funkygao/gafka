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
	wg         *sync.WaitGroup
	hostname   string
	dryRun     bool

	pubPools map[string]*pubPool // key is cluster
	poolLock sync.RWMutex

	lastRefreshedAt time.Time
}

func NewPubStore(wg *sync.WaitGroup, debug bool, dryRun bool) *pubStore {
	if debug {
		sarama.Logger = l.New(os.Stdout, color.Green("[Sarama]"),
			l.LstdFlags|l.Lshortfile)
	}

	return &pubStore{
		hostname:   ctx.Hostname(),
		pubPools:   make(map[string]*pubPool),
		wg:         wg,
		dryRun:     dryRun,
		shutdownCh: make(chan struct{}),
	}
}

func (this *pubStore) Start() (err error) {
	this.wg.Add(1)
	defer this.wg.Done()

	for _, cluster := range meta.Default.ClusterNames() {
		this.pubPools[cluster] = newPubPool(this,
			meta.Default.BrokerList(cluster), 200) // TODO
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
	this.poolLock.Lock()
	for _, p := range this.pubPools {
		p.Close()
	}
	this.poolLock.Unlock()

	close(this.shutdownCh)
}

func (this *pubStore) Name() string {
	return "kafka"
}

func (this *pubStore) doRefresh(force bool) {
	// TODO the lock is too big, should consider cluster level refresh
	this.poolLock.Lock()
	if time.Since(this.lastRefreshedAt) > time.Second*5 { // TODO
		if force {
			meta.Default.Refresh()
			log.Trace("all kafka clusters broker list refreshed")
		}

		for _, cluster := range meta.Default.ClusterNames() {
			this.pubPools[cluster].RefreshBrokerList(meta.Default.BrokerList(cluster))
		}

		this.lastRefreshedAt = time.Now()
	}
	this.poolLock.Unlock()
}

func (this *pubStore) SyncPub(cluster string, topic string, key []byte,
	msg []byte) (err error) {
	this.poolLock.RLock()
	pool, present := this.pubPools[cluster]
	this.poolLock.RUnlock()
	if !present {
		err = store.ErrInvalidCluster
		return
	}

	var (
		maxRetries = 5
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

	if this.dryRun {
		producer, err = pool.GetSyncProducer()
		producer.Recycle()
		return
	}

	for i := 0; i < maxRetries; i++ {
		producer, err = pool.GetSyncProducer()
		if err != nil {
			if producer != nil {
				producer.Recycle()
			}

			switch err {
			case ErrorKafkaConfig, sarama.ErrOutOfBrokers:
				this.doRefresh(true)

			default:
				log.Warn("unknown pubPool err: %v", err)
			}

			goto backoff
		}

		_, _, err = producer.SendMessage(producerMsg)
		producer.Recycle()
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
			log.Warn("unknown pub err: %v", err)

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

		log.Debug("cluster:%s topic:%s %v, retry in %v", cluster, topic, err, retryDelay)
		time.Sleep(retryDelay)
	}

	return
}

func (this *pubStore) AsyncPub(cluster string, topic string, key []byte,
	msg []byte) (err error) {
	this.poolLock.RLock()
	pool, present := this.pubPools[cluster]
	this.poolLock.RUnlock()
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
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	}
	producer.Recycle()
	return
}
