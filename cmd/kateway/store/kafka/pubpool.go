package kafka

import (
	"time"

	"github.com/funkygao/golib/set"
	log "github.com/funkygao/log4go"
	pool "github.com/youtube/vitess/go/pools"
	"golang.org/x/net/context"
)

type pubPool struct {
	store *pubStore

	size       int
	nextId     uint64
	brokerList []string

	syncPool  *pool.ResourcePool
	asyncPool *pool.ResourcePool
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

func (this *pubPool) initialize() {
	this.syncPool = pool.NewResourcePool(this.syncProducerFactory,
		this.size, this.size, time.Minute*10)
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
