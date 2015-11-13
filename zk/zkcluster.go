package zk

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
)

// ZkCluster is a kafka cluster that has a chroot path in Zookeeper.
type ZkCluster struct {
	zone *ZkZone
	name string // cluster name
	path string // cluster's kafka chroot path in zk cluster
}

func (this *ZkCluster) Name() string {
	return this.name
}

func (this *ZkCluster) Topics() []string {
	r := make([]string, 0)
	for name, _ := range this.zone.childrenWithData(this.path + BrokerTopicsPath) {
		r = append(r, name)
	}
	return r
}

// Returns {groupName: online}
func (this *ZkCluster) ConsumerGroups() map[string]bool {
	r := make(map[string]bool)
	for _, group := range this.zone.children(this.path + ConsumersPath) {
		if len(this.zone.children(this.path+ConsumersPath+"/"+group+"/ids")) > 0 {
			r[group] = true
		} else {
			r[group] = false
		}
	}
	return r
}

// returns {consumerGroup: consumerInfo}
func (this *ZkCluster) ConsumersByGroup() map[string][]Consumer {
	r := make(map[string][]Consumer)
	brokerList := this.BrokerList()
	if len(brokerList) == 0 {
		// no brokers alive, so cannot tell the consumer lags
		return r
	}

	// TODO zk coupled with kafka, bad design
	kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		log.Error("%+v %v", brokerList, err)
		return r
	}

	for group, online := range this.ConsumerGroups() {
		offsetsPath := this.path + ConsumersPath + "/" + group + "/offsets"
		topics := this.zone.children(offsetsPath)
		for _, topic := range topics {
			for partitionId, offsetData := range this.zone.childrenWithData(offsetsPath +
				"/" + topic) {
				consumerOffset, err := strconv.ParseInt(string(offsetData), 10, 64)
				if !this.zone.swallow(err) {
					return r
				}

				pid, err := strconv.Atoi(partitionId)
				if !this.zone.swallow(err) {
					return r
				}

				// found err: Request was for a topic or partition that does not exist on this broker.
				producerOffset, err := kfk.GetOffset(topic, int32(pid),
					sarama.OffsetNewest)
				if !this.zone.swallow(err) {
					return r
				}

				c := Consumer{
					Online:      online,
					Topic:       topic,
					PartitionId: partitionId,
					Offset:      consumerOffset,
					Lag:         producerOffset - consumerOffset,
				}
				if _, present := r[group]; !present {
					r[group] = make([]Consumer, 0)
				}
				r[group] = append(r[group], c)
			}
		}
	}
	return r
}

// returns {brokerId: broker}
func (this *ZkCluster) Brokers() map[string]*Broker {
	r := make(map[string]*Broker)
	for brokerId, brokerInfo := range this.zone.childrenWithData(this.path + BrokerIdsPath) {
		broker := newBroker(brokerId)
		broker.from(brokerInfo)

		r[brokerId] = broker
	}

	return r
}

func (this *ZkCluster) BrokerList() []string {
	r := make([]string, 0)
	for brokerId, brokerInfo := range this.zone.childrenWithData(this.path + BrokerIdsPath) {
		broker := newBroker(brokerId)
		broker.from(brokerInfo)
		r = append(r, broker.Addr())
	}
	return r
}

func (this *ZkCluster) Isr(topic string, partitionId int32) []int {
	partitionStateData, _ := this.zone.getData(fmt.Sprintf("%s%s/%s/partitions/%d/state", this.path, BrokerTopicsPath, topic,
		partitionId))
	partitionState := make(map[string]interface{})
	json.Unmarshal(partitionStateData, &partitionState)
	isr := partitionState["isr"].([]interface{})
	r := make([]int, 0, len(isr))
	for _, id := range isr {
		r = append(r, int(id.(float64)))
	}
	sort.Ints(r)

	return r
}

func (this *ZkCluster) Broker(id int) (b *Broker) {
	idStr := strconv.Itoa(id)
	zkData, _ := this.zone.getData(this.path + BrokerIdsPath +
		zkPathSeperator + idStr)
	b = newBroker(idStr)
	b.from(zkData)
	return
}
