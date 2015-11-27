package main

import (
	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"github.com/wvanbergen/kafka/consumergroup"
)

// TODO resume from last offset
func (this *Gateway) pickConsumerGroup(topic, group,
	clientId string) (cg *consumergroup.ConsumerGroup, err error) {
	this.cgLock.Lock()
	defer this.cgLock.Unlock()

	var present bool
	if _, present = this.consumerGroups[topic]; !present {
		this.consumerGroups[topic] = make(map[string]map[string]*consumergroup.ConsumerGroup)
	}
	if _, present = this.consumerGroups[topic][group]; !present {
		this.consumerGroups[topic][group] = make(map[string]*consumergroup.ConsumerGroup)
	}
	cg, present = this.consumerGroups[topic][group][clientId]
	if present {
		log.Debug("found cg for %s:%s:%s", topic, group, clientId)
		return
	}

	if len(this.consumerGroups[topic][group]) >= len(this.metaStore.Partitions(topic)) {
		err = ErrTooManyConsumers
		log.Error("topic:%s group:%s client:%s %v", topic, group, clientId, err)

		return
	}

	// create the consumer group for this client
	cf := consumergroup.NewConfig()
	cf.Zookeeper.Chroot = this.metaStore.ZkChroot()
	cf.ClientID = this.hostname
	cg, err = consumergroup.JoinConsumerGroup(group, []string{topic},
		this.metaStore.ZkAddrs(), cf)
	if err == nil {
		this.consumerGroups[topic][group][clientId] = cg
	}

	return
}

// TODO
func (this *Gateway) trackOffset(message *sarama.ConsumerMessage) {

}

// TODO
func (this *Gateway) runConsumerGroupsWatchdog() {

}
