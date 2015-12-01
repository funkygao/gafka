package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"github.com/wvanbergen/kafka/consumergroup"
)

type subPool struct {
	store *subStore

	clientMap     map[string]*consumergroup.ConsumerGroup // a client can only sub 1 topic
	clientMapLock sync.RWMutex                            // TODO the lock is too big

	rebalancing bool // FIXME 1 topic rebalance should not affect other topics
}

func newSubPool(store *subStore) *subPool {
	return &subPool{
		store:     store,
		clientMap: make(map[string]*consumergroup.ConsumerGroup),
	}
}

func (this *subPool) PickConsumerGroup(cluster, topic, group,
	remoteAddr string) (cg *consumergroup.ConsumerGroup, err error) {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	for retries := 0; retries < 3; retries++ {
		if this.rebalancing {
			time.Sleep(time.Millisecond * 300) // TODO
		} else {
			break
		}
	}

	if this.rebalancing {
		err = ErrRebalancing
		return
	}

	// FIXME 2 partition, if 3 client concurrently connects, got 3 consumer
	if this.store.meta.OnlineConsumersCount(topic, group) >= len(this.store.meta.Partitions(topic)) {
		err = ErrTooManyConsumers
		return
	}

	var present bool
	cg, present = this.clientMap[remoteAddr]
	if present {
		return
	}

	// cache miss, create the consumer group for this client
	cf := consumergroup.NewConfig()
	cf.ChannelBufferSize = 0
	cf.Offsets.Initial = sarama.OffsetOldest
	cf.Offsets.CommitInterval = time.Minute // TODO
	// time to wait for all the offsets for a partition to be processed after stopping to consume from it.
	cf.Offsets.ProcessingTimeout = time.Second * 10 // TODO
	cf.Zookeeper.Chroot = this.store.meta.ZkChroot()
	for i := 0; i < 3; i++ {
		// join group will async register zk owners znodes
		// so, if many client concurrently connects to kateway, will not
		// strictly throw ErrTooManyConsumers
		cg, err = consumergroup.JoinConsumerGroup(group, []string{topic},
			this.store.meta.ZkAddrs(), cf)
		if err == nil {
			this.clientMap[remoteAddr] = cg
			break
		}

		// backoff
		time.Sleep(time.Millisecond * 100)
	}

	return
}

func (this *subPool) killClient(remoteAddr string) {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	// TODO golang keep-alive max idle defaults 60s
	this.rebalancing = true
	if c, present := this.clientMap[remoteAddr]; present {
		c.Close() // will flush offset, must wait, otherwise offset is not guanranteed
	} else {
		// should never happen
		this.rebalancing = false
		log.Warn("consumer %s never consumed", remoteAddr)

		return
	}

	delete(this.clientMap, remoteAddr)
	this.rebalancing = false

	log.Info("consumer %s closed, rebalanced ok", remoteAddr)
}

func (this *subPool) Stop() {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	var wg sync.WaitGroup
	for _, c := range this.clientMap {
		wg.Add(1)
		go func() {
			c.Close()
			wg.Done()
		}()
	}

	// wait for all consumers commit offset
	wg.Wait()
}
