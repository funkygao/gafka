package main

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"github.com/wvanbergen/kafka/consumergroup"
)

type subPool struct {
	hostname  string
	metaStore MetaStore

	shutdownCh chan struct{}

	// {topic: {group: {clientId: consumerGroup}}}
	subPool map[string]map[string]map[string]*consumergroup.ConsumerGroup
	cgLock  sync.RWMutex

	// {topic: {group: {clientId: {partitionId: message}}}}
	consumerOffsets map[string]map[string]map[string]map[int32]*sarama.ConsumerMessage
	offsetsLock     sync.Mutex
}

func newSubPool(hostname string, metaStore MetaStore, shutdownCh chan struct{}) *subPool {
	return &subPool{
		hostname:        hostname,
		metaStore:       metaStore,
		shutdownCh:      shutdownCh,
		subPool:         make(map[string]map[string]map[string]*consumergroup.ConsumerGroup),
		consumerOffsets: make(map[string]map[string]map[string]map[int32]*sarama.ConsumerMessage),
	}
}

// TODO resume from last offset
func (this *subPool) PickConsumerGroup(topic, group,
	clientId string) (cg *consumergroup.ConsumerGroup, err error) {
	this.cgLock.Lock()
	defer this.cgLock.Unlock()

	var present bool
	if _, present = this.subPool[topic]; !present {
		this.subPool[topic] = make(map[string]map[string]*consumergroup.ConsumerGroup)
	}
	if _, present = this.subPool[topic][group]; !present {
		this.subPool[topic][group] = make(map[string]*consumergroup.ConsumerGroup)
	}
	cg, present = this.subPool[topic][group][clientId]
	if present {
		log.Debug("found cg for %s:%s:%s", topic, group, clientId)
		return
	}

	if len(this.subPool[topic][group]) >= len(this.metaStore.Partitions(topic)) {
		err = ErrTooManyConsumers
		log.Error("topic:%s group:%s client:%s %v", topic, group, clientId, err)

		return
	}

	// create the consumer group for this client
	cf := consumergroup.NewConfig()
	cf.Zookeeper.Chroot = this.metaStore.ZkChroot()
	for i := 0; i < 3; i++ {
		cg, err = consumergroup.JoinConsumerGroup(group, []string{topic},
			this.metaStore.ZkAddrs(), cf)
		if err == nil {
			this.subPool[topic][group][clientId] = cg
			break
		}
	}

	return
}

func (this *subPool) TrackOffset(topic, group, client string, message *sarama.ConsumerMessage) {
	this.offsetsLock.Lock()

	var present bool
	if _, present = this.consumerOffsets[topic]; !present {
		log.Info("new offset for {topic:%s}", topic)
		this.consumerOffsets[topic] = make(map[string]map[string]map[int32]*sarama.ConsumerMessage)
	}
	if _, present = this.consumerOffsets[topic][group]; !present {
		log.Info("new offset for {topic:%s, group:%s}", topic, group)
		this.consumerOffsets[topic][group] = make(map[string]map[int32]*sarama.ConsumerMessage)
	}
	if _, present = this.consumerOffsets[topic][group][client]; !present {
		log.Info("new offset for {topic:%s, group:%s, client:%s}", topic, group, client)
		this.consumerOffsets[topic][group][client] = make(map[int32]*sarama.ConsumerMessage)
	}

	this.consumerOffsets[message.Topic][group][client][message.Partition] = message
	this.offsetsLock.Unlock()
}

func (this *subPool) Start() {
	ticker := time.NewTicker(time.Minute) // TODO
	defer ticker.Stop()

	for {
		select {
		case <-this.shutdownCh:
			break

		case <-ticker.C:
			// TODO thundering herd
			// TODO what if the offsets not changed since last commit?
			for topic, ts := range this.consumerOffsets {
				for group, gs := range ts {
					for client, cs := range gs {
						for _, msg := range cs {
							this.subPool[topic][group][client].CommitUpto(msg)
							log.Info("{topic:%s, group:%s, partition:%d} offset commit: %d",
								topic, group, msg.Partition, msg.Offset)
						}
					}
				}
			}

		}
	}

}

func (this *subPool) Stop() {
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
}
