package kafka

import (
	l "log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

type pubStore struct {
	shutdownCh <-chan struct{}
	wg         *sync.WaitGroup
	hostname   string

	meta    meta.MetaStore
	pubPool *pubPool
}

func NewPubStore(meta meta.MetaStore, hostname string, wg *sync.WaitGroup,
	shutdownCh <-chan struct{}, debug bool) *pubStore {
	if debug {
		sarama.Logger = l.New(os.Stdout, color.Green("[Sarama]"),
			l.LstdFlags|l.Lshortfile)
	}

	return &pubStore{
		meta:       meta,
		hostname:   hostname,
		wg:         wg,
		shutdownCh: shutdownCh,
	}
}

func (this *pubStore) Start() (err error) {
	this.wg.Add(1)
	defer this.wg.Done()

	this.pubPool = newPubPool(this, this.meta.BrokerList())

	ticker := time.NewTicker(this.meta.RefreshInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// main thread triggers the refresh, child threads just get fed
			this.pubPool.RefreshBrokerList(this.meta.BrokerList())

		case <-this.shutdownCh:
			log.Info("kafka pub store stopped")
			this.pubPool.Stop()
			return

		}
	}

	return
}

func (this *pubStore) SyncPub(cluster string, topic, key string,
	msg []byte) (partition int32, offset int64, err error) {
	client, e := this.pubPool.Get()
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
	msg []byte) (err error) {
	client, e := this.pubPool.Get()
	if e != nil {
		if client != nil {
			client.Recycle()
		}
		return e
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
