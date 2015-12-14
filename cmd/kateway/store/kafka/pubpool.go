package kafka

import (
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/pool"
	"github.com/funkygao/golib/set"
	log "github.com/funkygao/log4go"
)

type syncProducerClient struct {
	id   uint64
	pool *pubPool

	client sarama.Client
	sarama.SyncProducer
}

func (this *syncProducerClient) Close() {
	log.Trace("closeing syncProducerClient: %d", this.id)
	this.SyncProducer.Close()
	this.client.Close()
	//this.pool.pool.Put(nil)
}

func (this *syncProducerClient) Id() uint64 {
	return this.id
}

func (this *syncProducerClient) IsOpen() bool {
	return !this.client.Closed()
}

func (this *syncProducerClient) Recycle() {
	if this.client.Closed() {
		this.pool.syncPool.Kill(this)
		this.pool.syncPool.Put(nil)
	} else {
		this.pool.syncPool.Put(this)
	}
}

type asyncProducerClient struct {
	id   uint64
	pool *pubPool

	client sarama.Client
	sarama.AsyncProducer
}

func (this *asyncProducerClient) Close() {
	log.Trace("closeing asyncProducerClient: %d", this.id)
	this.AsyncProducer.AsyncClose()
	this.client.Close()
	//this.pool.pool.Put(nil)
}

func (this *asyncProducerClient) Id() uint64 {
	return this.id
}

func (this *asyncProducerClient) IsOpen() bool {
	return !this.client.Closed()
}

func (this *asyncProducerClient) Recycle() {
	if this.client.Closed() {
		this.pool.asyncPool.Kill(this)
		this.pool.asyncPool.Put(nil)
	} else {
		this.pool.asyncPool.Put(this)
	}

}

type pubPool struct {
	store *pubStore

	size       int
	brokerList []string
	syncPool   *pool.ResourcePool
	asyncPool  *pool.ResourcePool
	nextId     uint64
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
	cf.Producer.Timeout = time.Second
	cf.ClientID = this.store.hostname
	//cf.Producer.Compression = sarama.CompressionSnappy
	cf.Producer.Retry.Max = 3
	apc.client, err = sarama.NewClient(this.brokerList, cf)
	if err != nil {
		return nil, err
	}

	log.Trace("kafka connected[%d]: %+v %s", apc.id, this.brokerList,
		time.Since(t1))

	apc.AsyncProducer, err = sarama.NewAsyncProducerFromClient(apc.client)
	if err != nil {
		return nil, err
	}

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
	cf.Producer.Timeout = time.Second
	cf.ClientID = this.store.hostname
	//cf.Producer.Compression = sarama.CompressionSnappy
	cf.Producer.Retry.Max = 3
	spc.client, err = sarama.NewClient(this.brokerList, cf)
	if err != nil {
		return nil, err
	}

	log.Trace("kafka connected[%d]: %+v %s", spc.id, this.brokerList,
		time.Since(t1))

	// TODO sarama.NewSyncProducer(this.brokerList, cf)
	// TODO we don't need reuse the sarama.Client
	spc.SyncProducer, err = sarama.NewSyncProducerFromClient(spc.client)
	if err != nil {
		return nil, err
	}

	return spc, err
}

func (this *pubPool) initialize() {
	this.syncPool = pool.NewResourcePool("kafka.pub.sync", this.syncProducerFactory,
		this.size, this.size, 0, time.Second*10, time.Minute) // TODO
	this.asyncPool = pool.NewResourcePool("kafka.pub.async", this.asyncProducerFactory,
		this.size, this.size, 0, time.Second*10, time.Minute)
}

func (this *pubPool) Close() {
	this.syncPool.Close()
	this.asyncPool.Close()
}

func (this *pubPool) Stop() {
	this.Close()
}

func (this *pubPool) GetSyncProducer() (*syncProducerClient, error) {
	k, err := this.syncPool.Get()
	if err != nil {
		return nil, err
	}

	return k.(*syncProducerClient), nil
}

func (this *pubPool) GetAsyncProducer() (*asyncProducerClient, error) {
	k, err := this.asyncPool.Get()
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
