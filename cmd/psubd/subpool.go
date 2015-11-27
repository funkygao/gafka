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
	cgs     map[string]map[string]map[string]*consumergroup.ConsumerGroup
	cgsLock sync.RWMutex
}

func newSubPool(gw *Gateway) *subPool {
	return &subPool{
		gw:  gw,
		cgs: make(map[string]map[string]map[string]*consumergroup.ConsumerGroup),
	}
}

// TODO resume from last offset
func (this *subPool) PickConsumerGroup(topic, group,
	client string) (cg *consumergroup.ConsumerGroup, err error) {
	this.cgsLock.Lock()
	defer this.cgsLock.Unlock()

	var present bool
	if _, present = this.cgs[topic]; !present {
		this.cgs[topic] = make(map[string]map[string]*consumergroup.ConsumerGroup)
	}
	if _, present = this.cgs[topic][group]; !present {
		this.cgs[topic][group] = make(map[string]*consumergroup.ConsumerGroup)
	}
	cg, present = this.cgs[topic][group][client]
	if present {
		return
	}

	if len(this.cgs[topic][group]) >= len(this.gw.metaStore.Partitions(topic)) {
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
			this.cgs[topic][group][client] = cg
			break
		}
	}

	return
}

func (this *subPool) KillClient(topic, group, client string) {
	this.cgs[topic][group][client].Close() // will flush offset
	delete(this.cgs[topic][group], client)
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
	for topic, ts := range this.cgs {
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
	this.cgs = make(map[string]map[string]map[string]*consumergroup.ConsumerGroup)
}
