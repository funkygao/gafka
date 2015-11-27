package main

import (
	"sync"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/wvanbergen/kafka/consumergroup"
)

type subPool struct {
	gw *Gateway

	// {topic: {group: {client: consumerGroup}}}
	subPool map[string]map[string]map[string]*consumergroup.ConsumerGroup
	cgLock  sync.RWMutex
}

func newSubPool(gw *Gateway) *subPool {
	return &subPool{
		gw:      gw,
		subPool: make(map[string]map[string]map[string]*consumergroup.ConsumerGroup),
	}
}

// TODO resume from last offset
func (this *subPool) PickConsumerGroup(topic, group,
	client string) (cg *consumergroup.ConsumerGroup, err error) {
	this.cgLock.Lock()
	defer this.cgLock.Unlock()

	var present bool
	if _, present = this.subPool[topic]; !present {
		this.subPool[topic] = make(map[string]map[string]*consumergroup.ConsumerGroup)
	}
	if _, present = this.subPool[topic][group]; !present {
		this.subPool[topic][group] = make(map[string]*consumergroup.ConsumerGroup)
	}
	cg, present = this.subPool[topic][group][client]
	if present {
		return
	}

	if len(this.subPool[topic][group]) >= len(this.gw.metaStore.Partitions(topic)) {
		err = ErrTooManyConsumers

		return
	}

	// create the consumer group for this client
	cf := consumergroup.NewConfig()
	//cf.Offsets.Initial = sarama.OffsetNewest // TODO
	cf.Offsets.CommitInterval = time.Minute // TODO
	cf.Offsets.ProcessingTimeout = time.Minute
	cf.Zookeeper.Chroot = this.gw.metaStore.ZkChroot()
	for i := 0; i < 3; i++ {
		cg, err = consumergroup.JoinConsumerGroup(group, []string{topic},
			this.gw.metaStore.ZkAddrs(), cf)
		if err == nil {
			this.subPool[topic][group][client] = cg
			break
		}
	}

	return
}

func (this *subPool) Start() {
	for {
		select {
		case <-this.gw.shutdownCh:
			log.Info("sub pool shutdown")
			this.Stop()
			this.gw.wg.Done()
			break

		}
	}

}

func (this *subPool) Stop() {
	// TODO flush the offsets
	for topic, ts := range this.subPool {
		for group, gs := range ts {
			for client, c := range gs {
				if err := c.Close(); err != nil {
					log.Error("{topic:%s, group:%s, client:%s}: %v", topic,
						group, client, err)
				}
			}
		}
	}

	// reinit the vars
	this.subPool = make(map[string]map[string]map[string]*consumergroup.ConsumerGroup)
}
