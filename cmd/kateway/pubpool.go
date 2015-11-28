package main

import (
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/golib/pool"
	"github.com/funkygao/golib/set"
	log "github.com/funkygao/log4go"
)

type pubClient struct {
	id   uint64
	pool *pubPool

	sarama.Client
}

func (this *pubClient) Close() {
	log.Debug("closeing pubClient: %d", this.id)
	this.Client.Close()
	//this.pool.pool.Put(nil)
}

func (this *pubClient) Id() uint64 {
	return this.id
}

func (this *pubClient) IsOpen() bool {
	return !this.Client.Closed()
}

func (this *pubClient) Recycle() {
	if this.Client.Closed() {
		this.pool.pool.Kill(this)
		this.pool.pool.Put(nil)
	} else {
		this.pool.pool.Put(this)
	}

}

type pubPool struct {
	gw *Gateway

	brokerList []string
	pool       *pool.ResourcePool
	nextId     uint64
}

func newPubPool(gw *Gateway, brokerList []string) *pubPool {
	this := &pubPool{
		brokerList: brokerList,
		gw:         gw,
	}
	this.initialize()

	return this
}

func (this *pubPool) Start() {
	this.gw.wg.Add(1)
	defer this.gw.wg.Done()

	ever := true
	for ever {
		select {
		case <-this.gw.shutdownCh:
			log.Info("pub pool shutdown")
			this.Stop()
			ever = false

		}
	}
}

func (this *pubPool) initialize() {
	factory := func() (pool.Resource, error) {
		conn := &pubClient{
			pool: this,
			id:   atomic.AddUint64(&this.nextId, 1),
		}

		var err error
		t1 := time.Now()
		cf := sarama.NewConfig()
		cf.Producer.RequiredAcks = sarama.WaitForLocal
		cf.Producer.Partitioner = sarama.NewHashPartitioner
		cf.Producer.Timeout = time.Second
		cf.ClientID = this.gw.hostname
		//cf.Producer.Compression = sarama.CompressionSnappy
		cf.Producer.Retry.Max = 3
		conn.Client, err = sarama.NewClient(this.brokerList, cf)
		if err == nil {
			log.Debug("kafka connected[%d]: %+v %s", conn.id, this.brokerList,
				time.Since(t1))
		}

		return conn, err
	}

	this.pool = pool.NewResourcePool("kafka", factory,
		1000, 1000, 0, time.Second*10, time.Minute) // TODO
}

func (this *pubPool) Close() {
	this.pool.Close()
}

func (this *pubPool) Stop() {
	this.Close()
}

func (this *pubPool) Get() (*pubClient, error) {
	k, err := this.pool.Get()
	if err != nil {
		return nil, err
	}

	return k.(*pubClient), nil
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
		this.pool.Close()
		this.initialize()
	}

}
