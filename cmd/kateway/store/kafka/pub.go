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
		this.pubPools[cluster] = newPubPool(this,
			meta.Default.BrokerList(cluster), 500) // TODO
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

	producer, e := pool.GetSyncProducer()
	if e != nil {
		if producer != nil {
			producer.Recycle()
		}

		return -1, -1, e
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

	producer.Recycle()
	return
}

func (this *pubStore) AsyncPub(cluster string, topic, key string,
	msg []byte) (partition int32, offset int64, err error) {
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

		return 0, 0, e
	}

	var keyEncoder sarama.Encoder = nil // will use random partitioner
	if key != "" {
		keyEncoder = sarama.StringEncoder(key) // will use hash partition
	}
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	}
	producer.Recycle()
	return
}
