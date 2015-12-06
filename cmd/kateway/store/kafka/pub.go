package kafka

import (
	l "log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/color"
	log "github.com/funkygao/log4go"
)

type pubStore struct {
	shutdownCh <-chan struct{}
	wg         *sync.WaitGroup
	hostname   string

	pubPool *pubPool // FIXME maybe we should have another pool for async
}

func NewPubStore(wg *sync.WaitGroup, shutdownCh <-chan struct{}, debug bool) *pubStore {
	if debug {
		sarama.Logger = l.New(os.Stdout, color.Green("[Sarama]"),
			l.LstdFlags|l.Lshortfile)
	}

	return &pubStore{
		hostname:   ctx.Hostname(),
		wg:         wg,
		shutdownCh: shutdownCh,
	}
}

func (this *pubStore) Start() (err error) {
	this.wg.Add(1)
	defer this.wg.Done()

	this.pubPool = newPubPool(this, meta.Default.BrokerList())

	ticker := time.NewTicker(meta.Default.RefreshInterval())
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				// TODO maybe this is not neccessary
				// main thread triggers the refresh, child threads just get fed
				this.pubPool.RefreshBrokerList(meta.Default.BrokerList())

			case <-this.shutdownCh:
				this.pubPool.Stop()
				log.Trace("kafka pub store stopped")
				return

			}
		}
	}()

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
	msg []byte) (partition int32, offset int64, err error) {
	client, e := this.pubPool.Get()
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
