package main

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"github.com/wvanbergen/kafka/consumergroup"
)

type subPool struct {
	gw *Gateway

	clientMap     map[string]*consumergroup.ConsumerGroup // a client can only sub 1 topic
	clientMapLock sync.RWMutex                            // TODO the lock is too big

	rebalancing bool
}

func newSubPool(gw *Gateway) *subPool {
	return &subPool{
		gw:        gw,
		clientMap: make(map[string]*consumergroup.ConsumerGroup),
	}
}

func (this *subPool) PickConsumerGroup(ver, topic, group,
	client string) (cg *consumergroup.ConsumerGroup, err error) {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	if this.rebalancing {
		err = ErrRebalancing
		return
	}

	// FIXME 2 partition, if 3 client concurrently connects, got 3 consumer
	if this.gw.metaStore.OnlineConsumersCount(topic, group) >= len(this.gw.metaStore.Partitions(topic)) {
		err = ErrTooManyConsumers
		return
	}

	var present bool
	cg, present = this.clientMap[client]
	if present {
		return
	}

	// cache miss, create the consumer group for this client
	cf := consumergroup.NewConfig()
	cf.ChannelBufferSize = 0
	cf.Offsets.Initial = sarama.OffsetOldest
	cf.Offsets.CommitInterval = options.offsetCommitInterval
	// time to wait for all the offsets for a partition to be processed after stopping to consume from it.
	cf.Offsets.ProcessingTimeout = time.Second * 10 // TODO
	cf.Zookeeper.Chroot = this.gw.metaStore.ZkChroot()
	for i := 0; i < 3; i++ {
		// join group will async register zk owners znodes
		// so, if many client concurrently connects to kateway, will not
		// strictly throw ErrTooManyConsumers
		cg, err = consumergroup.JoinConsumerGroup(group, []string{topic},
			this.gw.metaStore.ZkAddrs(), cf)
		if err == nil {
			this.clientMap[client] = cg
			break
		}

		// backoff
		time.Sleep(time.Millisecond * 100)
	}

	return
}

func (this *subPool) KillClient(client string) {
	this.clientMapLock.Lock()
	defer this.clientMapLock.Unlock()

	// TODO golang keep-alive max idle defaults 60s
	this.rebalancing = true
	if c, present := this.clientMap[client]; present {
		c.Close() // will flush offset, must wait, otherwise offset is not guanranteed
	} else {
		// should never happen
		this.rebalancing = false
		log.Warn("consumer %s never consumed", client)

		return
	}

	delete(this.clientMap, client)

	this.rebalancing = false

	log.Info("consumer %s closed, rebalanced ok", client)
}

func (this *subPool) Start() {
	this.gw.wg.Add(1)
	defer this.gw.wg.Done()

	ever := true
	for ever {
		select {
		case <-this.gw.shutdownCh:
			log.Info("sub pool shutdown")
			this.Stop()
			ever = false

		case remoteAddr := <-this.gw.subServer.closedConnCh: // TODO
			this.KillClient(remoteAddr)
		}
	}

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
