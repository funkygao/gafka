package kafka

import (
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/set"
	log "github.com/funkygao/log4go"
	pool "github.com/youtube/vitess/go/pools"
	"golang.org/x/net/context"
)

type syncProducerClient struct {
	pool *pubPool

	id uint64
	sarama.SyncProducer
}

func (this *syncProducerClient) Close() {
	log.Trace("closing kafka sync client: %d", this.id)
	this.SyncProducer.Close()
	//this.pool.syncPool.Put(nil) // fill this slot
}

func (this *syncProducerClient) Id() uint64 {
	return this.id
}

func (this *syncProducerClient) Recycle() {
	this.pool.syncPool.Put(this)
}

type asyncProducerClient struct {
	pool *pubPool

	id uint64
	sarama.AsyncProducer
}

func (this *asyncProducerClient) Close() {
	log.Trace("closing kafka async client: %d", this.id)
	this.AsyncProducer.AsyncClose()
}

func (this *asyncProducerClient) Id() uint64 {
	return this.id
}

func (this *asyncProducerClient) Recycle() {
	this.pool.asyncPool.Put(this)
}

type pubPool struct {
	store *pubStore

	size       int
	nextId     uint64
	brokerList []string
	syncPool   *pool.ResourcePool
	asyncPool  *pool.ResourcePool
}

func newPubPool(store *pubStore, brokerList []string, size int) *pubPool {
	this := &pubPool{
		store:      store,
		size:       size,
		brokerList: brokerList,
	}
	this.initialize()

	return this
}

func (this *pubPool) asyncProducerFactory() (pool.Resource, error) {
	apc := &asyncProducerClient{
		pool: this,
		id:   atomic.AddUint64(&this.nextId, 1),
	}

	var err error
	t1 := time.Now()
	cf := sarama.NewConfig()
	cf.Metadata.RefreshFrequency = time.Minute * 10 // TODO
	cf.Metadata.Retry.Max = 3
	cf.Producer.Flush.Frequency = time.Second * 10
	cf.Producer.Flush.Messages = 1000
	cf.Producer.Flush.MaxMessages = 1000
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second * 4
	cf.ClientID = this.store.hostname
	cf.Producer.Retry.Max = 3
	//cf.Producer.Compression = sarama.CompressionSnappy

	apc.AsyncProducer, err = sarama.NewAsyncProducer(this.brokerList, cf)
	if err != nil {
		return nil, err
	}

	log.Trace("kafka connected[%d]: %+v %s", apc.id, this.brokerList,
		time.Since(t1))

	// TODO
	go func() {
		// messages will only be returned here after all retry attempts are exhausted.
		for err := range apc.Errors() {
			log.Error("async producer: %v", err)
		}
	}()

	return apc, err
}

func (this *pubPool) syncProducerFactory() (pool.Resource, error) {
	spc := &syncProducerClient{
		pool: this,
		id:   atomic.AddUint64(&this.nextId, 1),
	}

	var err error
	t1 := time.Now()
	cf := sarama.NewConfig()
	cf.Metadata.RefreshFrequency = time.Minute * 10 // TODO
	cf.Metadata.Retry.Max = 3
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second * 4
	cf.ClientID = this.store.hostname
	cf.Producer.Retry.Max = 3
	//cf.Producer.Compression = sarama.CompressionSnappy

	spc.SyncProducer, err = sarama.NewSyncProducer(this.brokerList, cf)
	if err != nil {
		return nil, err
	}

	log.Trace("kafka connected[%d]: %+v %s", spc.id, this.brokerList,
		time.Since(t1))

	return spc, err
}

func (this *pubPool) initialize() {
	this.syncPool = pool.NewResourcePool(this.syncProducerFactory,
		this.size, this.size, time.Minute*10) // TODO
	this.asyncPool = pool.NewResourcePool(this.asyncProducerFactory,
		this.size, this.size, time.Minute*10)
}

func (this *pubPool) Close() {
	this.syncPool.Close()
	this.syncPool = nil

	this.asyncPool.Close()
	this.asyncPool = nil
}

func (this *pubPool) GetSyncProducer() (*syncProducerClient, error) {
	ctx := context.Background()
	k, err := this.syncPool.Get(ctx)
	if err != nil {
		return nil, err
	}

	return k.(*syncProducerClient), nil
}

func (this *pubPool) GetAsyncProducer() (*asyncProducerClient, error) {
	ctx := context.Background()
	k, err := this.asyncPool.Get(ctx)
	if err != nil {
		return nil, err
	}

	return k.(*asyncProducerClient), nil
}

func (this *pubPool) RefreshBrokerList(brokerList []string) {
	setOld, setNew := set.NewSet(), set.NewSet()
	for _, b := range this.brokerList {
		setOld.Add(b)
	}
	for _, b := range brokerList {
		setNew.Add(b)
	}

	if !setOld.Equal(setNew) {
		log.Warn("brokers changed: %+v -> %+v", this.brokerList, brokerList)

		// rebuild the kafka conn pool
		this.brokerList = brokerList
		this.Close()
		this.initialize()
	}

}
