package zk

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

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

func (this *ZkCluster) ZkAddrs() string {
	return this.zone.ZkAddrs() + this.path
}

func (this *ZkCluster) Topics() []string {
	r := make([]string, 0)
	for name, _ := range this.zone.childrenWithData(this.topicsRoot()) {
		r = append(r, name)
	}
	return r
}

// Returns {groupName: online}
func (this *ZkCluster) ConsumerGroups() map[string][]*ConsumerZnode {
	r := make(map[string][]*ConsumerZnode)
	for _, group := range this.zone.children(this.consumerGroupsRoot()) {
		r[group] = make([]*ConsumerZnode, 0)
		for consumerId, data := range this.zone.childrenWithData(this.consumerGroupIdsPath(group)) {
			c := newConsumerZnode(consumerId)
			c.from(data.data)
			r[group] = append(r[group], c)
		}
	}
	return r
}

// returns {consumerGroup: consumerInfo}
func (this *ZkCluster) ConsumersByGroup(groupPrefix string) map[string][]Consumer {
	r := make(map[string][]Consumer)
	brokerList := this.BrokerList()
	if len(brokerList) == 0 {
		// no brokers alive, so cannot tell the consumer lags
		return r
	}

	// TODO zk coupled with kafka, bad design
	kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		log.Error("%s %+v %v", this.name, brokerList, err)
		return r
	}

	for group, consumers := range this.ConsumerGroups() {
		if groupPrefix != "" && !strings.Contains(group, groupPrefix) {
			continue
		}

		topics := this.zone.children(this.consumerGroupOffsetPath(group))
		for _, topic := range topics {
		topicLoop:
			for partitionId, offsetData := range this.zone.childrenWithData(this.consumerGroupOffsetOfTopicPath(group, topic)) {
				consumerOffset, err := strconv.ParseInt(string(offsetData.data), 10, 64)
				if !this.zone.swallow(err) {
					return r
				}

				pid, err := strconv.Atoi(partitionId)
				if !this.zone.swallow(err) {
					return r
				}

				producerOffset, err := kfk.GetOffset(topic, int32(pid),
					sarama.OffsetNewest)
				if err != nil {
					switch err {
					case sarama.ErrUnknownTopicOrPartition:
						// consumer is consuming a non-exist topic
						log.Warn("%s %s invalid topic[%s] partition:%s",
							this.name, group, topic, partitionId)
						continue topicLoop

					default:
						panic(err)
					}
				}

				c := Consumer{
					Group:          group,
					Online:         len(consumers) > 0,
					Topic:          topic,
					PartitionId:    partitionId,
					Timestamp:      offsetData.mtime,
					ConsumerOffset: consumerOffset,
					ProducerOffset: producerOffset,
					Lag:            producerOffset - consumerOffset,
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
func (this *ZkCluster) Brokers() map[string]*BrokerZnode {
	r := make(map[string]*BrokerZnode)
	for brokerId, brokerInfo := range this.zone.childrenWithData(this.brokerIdsRoot()) {
		broker := newBrokerZnode(brokerId)
		broker.from(brokerInfo.data)

		r[brokerId] = broker
	}

	return r
}

func (this *ZkCluster) BrokerList() []string {
	r := make([]string, 0)
	for _, broker := range this.Brokers() {
		r = append(r, broker.Addr())
	}

	return r
}

func (this *ZkCluster) Isr(topic string, partitionId int32) []int {
	partitionStateData, _, _ := this.zone.conn.Get(this.partitionStatePath(topic, partitionId))
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

func (this *ZkCluster) Broker(id int) (b *BrokerZnode) {
	zkData, _, _ := this.zone.conn.Get(this.brokerPath(id))
	b = newBrokerZnode(strconv.Itoa(id))
	b.from(zkData)
	return
}
