package kafka

import (
	l "log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
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

	// FIXME maybe we should have another pool for async
	pubPools map[string]*pubPool // key is cluster
	poolLock sync.RWMutex
}

func NewPubStore(wg *sync.WaitGroup, debug bool) *pubStore {
	if debug {
		sarama.Logger = l.New(os.Stdout, color.Green("[Sarama]"),
			l.LstdFlags|l.Lshortfile)
	}

	return &pubStore{
		hostname:   ctx.Hostname(),
		pubPools:   make(map[string]*pubPool),
		wg:         wg,
		shutdownCh: make(chan struct{}),
	}
}

func (this *pubStore) Start() (err error) {
	this.wg.Add(1)
	defer this.wg.Done()

	for _, cluster := range meta.Default.ClusterNames() {
		this.pubPools[cluster] = newPubPool(this, meta.Default.BrokerList(cluster), 500)
	}

	go func() {
		// 5 seconds after meta store refresh
		time.Sleep(time.Second * 5)

		ticker := time.NewTicker(meta.Default.RefreshInterval())
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				this.doRefresh()

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
		p.Stop()
	}
	this.poolLock.Unlock()

	close(this.shutdownCh)
}

func (this *pubStore) Name() string {
	return "kafka"
}

func (this *pubStore) doRefresh() {
	// TODO maybe this is not neccessary
	// main thread triggers the refresh, child threads just get fed
	this.poolLock.Lock()
	for _, cluster := range meta.Default.ClusterNames() {
		this.pubPools[cluster].RefreshBrokerList(meta.Default.BrokerList(cluster))
	}
	this.poolLock.Unlock()
}

func (this *pubStore) SyncPub(cluster string, topic, key string,
	msg []byte) (partition int32, offset int64, err error) {
	this.poolLock.RLock()
	pool, present := this.pubPools[cluster]
	this.poolLock.RUnlock()

	if !present {
		err = store.ErrInvalidCluster
		return
	}

	client, e := pool.Get()
	if e != nil {
		if client != nil {
			client.Recycle()
		}
		return -1, -1, e
	}

	var producer sarama.SyncProducer
	producer, err = sarama.NewSyncProducerFromClient(client.Client)
	if err != nil {
		client.Recycle()
		return
	}

	// TODO add msg header

	var keyEncoder sarama.Encoder = nil // will use random partitioner
	if key != "" {
		keyEncoder = sarama.StringEncoder(key) // will use hash partition
	}
	partition, offset, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	})

	producer.Close() // TODO keep the conn open
	client.Recycle()
	return
}

func (this *pubStore) AsyncPub(cluster string, topic, key string,
	msg []byte) (partition int32, offset int64, err error) {
	this.poolLock.RLock()
	pool := this.pubPools[cluster]
	this.poolLock.RUnlock()

	client, e := pool.Get()
	if e != nil {
		if client != nil {
			client.Recycle()
		}
		return 0, 0, e
	}

	var producer sarama.AsyncProducer
	producer, err = sarama.NewAsyncProducerFromClient(client.Client)
	if err != nil {
		client.Recycle()
		return
	}

	// TODO pool up the error collector goroutines
	// messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Error("async producer:", err)
		}
	}()

	var keyEncoder sarama.Encoder = nil // will use random partitioner
	if key != "" {
		keyEncoder = sarama.StringEncoder(key) // will use hash partition
		log.Debug("keyed message: %s", key)
	}
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	}
	return
}
