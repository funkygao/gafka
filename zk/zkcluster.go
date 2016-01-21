package zk

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/pipestream"
	log "github.com/funkygao/log4go"
	"github.com/samuel/go-zookeeper/zk"
)

// ZkCluster is a kafka cluster that has a chroot path in Zookeeper.
type ZkCluster struct {
	zone *ZkZone
	name string // cluster name
	path string // cluster's kafka chroot path in zk cluster

	Nickname  string       `json:"nickname"`
	Roster    []BrokerInfo `json:"roster"` // manually registered brokers
	Replicas  int          `json:"replicas"`
	Priority  int          `json:"priority"`
	Public    bool         `json:"public"`
	Retention int          `json:"retention"` // in hours
}

func (this *ZkCluster) Name() string {
	return this.name
}

func (this *ZkCluster) Chroot() string {
	return this.path
}

// kafka servers.properties zookeeper.connect=
func (this *ZkCluster) ZkConnectAddr() string {
	return this.zone.ZkAddrs() + this.path
}

func (this *ZkCluster) NamedZkConnectAddr() string {
	return ctx.NamedZoneZkAddrs(this.zone.Name()) + this.path
}

func (this *ZkCluster) ZkZone() *ZkZone {
	return this.zone
}

func (this *ZkCluster) Close() {
	this.zone.Close()
}

func (this *ZkCluster) TopicsCtime() map[string]time.Time {
	r := make(map[string]time.Time)
	for name, data := range this.zone.ChildrenWithData(this.topicsRoot()) {
		r[name] = data.ctime.Time()
	}
	return r
}

func (this *ZkCluster) Partitions(topic string) []int32 {
	partitions := this.zone.children(this.partitionsPath(topic))
	r := make([]int32, 0, len(partitions))
	for _, p := range partitions {
		id, _ := strconv.Atoi(p)
		r = append(r, int32(id))
	}
	return r
}

func (this *ZkCluster) writeInfo(zc ZkCluster) error {
	// ensure parent path exists
	this.zone.createZnode(clusterInfoRoot, []byte(""))

	data, err := json.Marshal(zc)
	this.zone.swallow(err)

	_, err = this.zone.conn.Set(this.ClusterInfoPath(), data, -1)
	if err == zk.ErrNoNode {
		// create the node
		return this.zone.createZnode(this.ClusterInfoPath(), data)
	}

	return err
}

// Get registered cluster info from zk.
func (this *ZkCluster) RegisteredInfo() ZkCluster {
	zdata, _, err := this.zone.conn.Get(this.ClusterInfoPath())
	if err != nil {
		if err == zk.ErrNoNode {
			this.writeInfo(*this)
			return *this
		} else {
			this.zone.swallow(err)
		}

		return *this
	}

	var cluster ZkCluster
	json.Unmarshal(zdata, &cluster)
	cluster.name = this.name
	cluster.path = this.path
	cluster.zone = this.zone
	return cluster
}

func (this *ZkCluster) SetNickname(name string) {
	c := this.RegisteredInfo()
	c.Nickname = name
	data, _ := json.Marshal(c)
	this.zone.swallow(this.zone.setZnode(this.ClusterInfoPath(), data))
}

func (this *ZkCluster) SetRetention(retention int) {
	c := this.RegisteredInfo()
	c.Retention = retention
	data, _ := json.Marshal(c)
	this.zone.swallow(this.zone.setZnode(this.ClusterInfoPath(), data))
}

func (this *ZkCluster) SetPriority(priority int) {
	c := this.RegisteredInfo()
	c.Priority = priority
	data, _ := json.Marshal(c)
	this.zone.swallow(this.zone.setZnode(this.ClusterInfoPath(), data))
}

func (this *ZkCluster) SetReplicas(replicas int) {
	c := this.RegisteredInfo()
	c.Replicas = replicas
	data, _ := json.Marshal(c)
	this.zone.swallow(this.zone.setZnode(this.ClusterInfoPath(), data))
}

func (this *ZkCluster) SetPublic(public bool) {
	c := this.RegisteredInfo()
	c.Public = public
	data, _ := json.Marshal(c)
	this.zone.swallow(this.zone.setZnode(this.ClusterInfoPath(), data))
}

func (this *ZkCluster) RegisterBroker(id int, host string, port int) error {
	c := this.RegisteredInfo()
	for _, info := range c.Roster {
		if id == info.Id {
			return fmt.Errorf("dup broker id: %d", id)
		}

		if info.Host == host && info.Port == port {
			// dup broker adding
			return fmt.Errorf("dup host and port: %s:%d", host, port)
		}
	}

	b := BrokerInfo{Id: id, Host: host, Port: port}
	c.Roster = append(c.Roster, b)
	data, _ := json.Marshal(c)
	return this.zone.setZnode(this.ClusterInfoPath(), data)
}

// Returns {groupName: {consumerId: consumer}}
func (this *ZkCluster) ConsumerGroups() map[string]map[string]*ConsumerZnode {
	r := make(map[string]map[string]*ConsumerZnode)
	for _, group := range this.zone.children(this.consumerGroupsRoot()) {
		r[group] = make(map[string]*ConsumerZnode)
		for consumerId, data := range this.zone.ChildrenWithData(this.consumerGroupIdsPath(group)) {
			c := newConsumerZnode(consumerId)
			c.from(data.data)
			r[group][consumerId] = c
		}
	}
	return r
}

// Returns {partitionId: consumerId}
// consumerId is /consumers/$group/ids/$consumerId
func (this *ZkCluster) ownersOfGroupByTopic(group, topic string) map[string]string {
	r := make(map[string]string)
	for partition, data := range this.zone.ChildrenWithData(this.consumerGroupOwnerOfTopicPath(group, topic)) {
		// data:
		// for java api: $consumerId-$threadNum  /consumers/$group/ids/$group_$hostname-$timestamp-$uuid
		// for golang api: $consumerId
		consumerIdNum := string(data.data)
		var i int
		if strings.Contains(consumerIdNum, "_") {
			// FIXME this rule is too naive, not robust
			// java api consumer
			for i = len(consumerIdNum) - 1; consumerIdNum[i] != '-'; i-- {
			}
			r[partition] = consumerIdNum[:i]
		} else {
			// golang consumer
			r[partition] = consumerIdNum
		}

	}
	return r
}

// returns {consumerGroup: consumerInfo}
func (this *ZkCluster) ConsumersByGroup(groupPattern string) map[string][]ConsumerMeta {
	r := make(map[string][]ConsumerMeta)
	brokerList := this.BrokerList()
	if len(brokerList) == 0 {
		// no brokers alive, so cannot tell the consumer lags
		return r
	}

	// TODO zk coupled with kafka, bad design
	kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
	if err != nil {
		log.Error("kafka[%s] %+v: %v", this.name, brokerList, err)
		return r
	}

	consumerGroups := this.ConsumerGroups()
	for group, consumers := range consumerGroups {
		if groupPattern != "" && !strings.Contains(group, groupPattern) {
			continue
		}

		topics := this.zone.children(this.ConsumerGroupOffsetPath(group))
		for _, topic := range topics {
			consumerInstances := this.ownersOfGroupByTopic(group, topic)
			if len(consumerInstances) == 0 {
				// no online consumers running
				continue
			}

		topicLoop:
			for partitionId, offsetData := range this.zone.ChildrenWithData(this.consumerGroupOffsetOfTopicPath(group, topic)) {
				consumerOffset, err := strconv.ParseInt(string(offsetData.data), 10, 64)
				if err != nil {
					log.Error("kafka[%s] %s P:%s %v", this.name, topic, partitionId, err)
					continue topicLoop
				}

				pid, err := strconv.Atoi(partitionId)
				if err != nil {
					panic(err)
				}

				producerOffset, err := kfk.GetOffset(topic, int32(pid), sarama.OffsetNewest)
				if err != nil {
					switch err {
					case sarama.ErrUnknownTopicOrPartition:
						// consumer is consuming a non-exist topic
						log.Warn("kafka[%s] %s invalid topic[%s] partition:%s",
							this.name, group, topic, partitionId)
						continue topicLoop

					default:
						panic(err)
					}
				}

				cm := ConsumerMeta{
					Group:          group,
					Online:         len(consumers) > 0,
					Topic:          topic,
					PartitionId:    partitionId,
					Mtime:          offsetData.mtime,
					ConsumerZnode:  consumers[consumerInstances[partitionId]],
					ConsumerOffset: consumerOffset,
					ProducerOffset: producerOffset,
					Lag:            producerOffset - consumerOffset,
				}
				if _, present := r[group]; !present {
					r[group] = make([]ConsumerMeta, 0)
				}
				r[group] = append(r[group], cm)
			}
		}
	}
	return r
}

// Returns online {brokerId: broker}.
func (this *ZkCluster) Brokers() map[string]*BrokerZnode {
	r := make(map[string]*BrokerZnode)
	for brokerId, brokerInfo := range this.zone.ChildrenWithData(this.brokerIdsRoot()) {
		broker := newBrokerZnode(brokerId)
		broker.from(brokerInfo.data)

		r[brokerId] = broker
	}

	return r
}

// Returns distinct online consumers in group for a topic.
func (this *ZkCluster) OnlineConsumersCount(topic, group string) int {
	consumers := make(map[string]struct{})
	for _, zkData := range this.zone.ChildrenWithData(this.consumerGroupOwnerOfTopicPath(group, topic)) {
		consumers[string(zkData.data)] = struct{}{}
	}
	return len(consumers)
}

func (this *ZkCluster) NamedBrokerList() []string {
	r := make([]string, 0)
	for _, broker := range this.Brokers() {
		r = append(r, broker.NamedAddr())
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

func (this *ZkCluster) AddTopic(topic string, replicas,
	partitions int) (output []string, err error) {
	if replicas < 1 || partitions < 1 {
		return nil, errors.New("invalid replicas or partitions")
	}

	zkAddrs := this.ZkConnectAddr()
	cmd := pipestream.New(fmt.Sprintf("%s/bin/kafka-topics.sh", ctx.KafkaHome()),
		fmt.Sprintf("--zookeeper %s", zkAddrs),
		fmt.Sprintf("--create"),
		fmt.Sprintf("--topic %s", topic),
		fmt.Sprintf("--partitions %d", partitions),
		fmt.Sprintf("--replication-factor %d", replicas),
	)
	err = cmd.Open()
	if err != nil {
		return
	}
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)

	output = make([]string, 0)
	for scanner.Scan() {
		output = append(output, scanner.Text())
	}
	err = scanner.Err()
	if err != nil {
		return
	}

	return
}

func (this *ZkCluster) TotalConsumerOffsets(topicPattern string) (total int64) {
	// /$cluster/consumers/$group/offsets/$topic/0
	root := this.consumerGroupsRoot()
	groups := this.zone.children(root)
	for _, group := range groups {
		topicsPath := fmt.Sprintf("%s/%s/offsets", root, group)
		topics := this.zone.children(topicsPath)
		for _, topic := range topics {
			if topicPattern != "" && !strings.Contains(topic, topicPattern) {
				continue
			}

			offsetsPath := fmt.Sprintf("%s/%s", topicsPath, topic)
			offsets := this.zone.ChildrenWithData(offsetsPath)
			for _, zdata := range offsets {
				offset, _ := strconv.Atoi(string(zdata.data))
				total += int64(offset)
			}
		}
	}
	return
}

func (this *ZkCluster) ResetConsumerGroupOffset(topic, group string) {
	// TODO
}

func (this *ZkCluster) ListChildren(recursive bool) ([]string, error) {
	excludedPaths := map[string]struct{}{
		"/zookeeper": struct{}{},
	}
	result := make([]string, 0, 100)
	queue := list.New()
	queue.PushBack(this.path)
MAIN_LOOP:
	for {
		if queue.Len() == 0 {
			break
		}

		element := queue.Back()
		path := element.Value.(string)
		queue.Remove(element)

		children, _, err := this.zone.conn.Children(path)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("path[%s]: %v", path, err))
		}

		for _, child := range children {
			var p string
			if path == "/" {
				p = path + child
			} else {
				p = path + "/" + child
			}

			if _, present := excludedPaths[p]; present {
				continue
			}

			result = append(result, p)
			queue.PushBack(p)
		}

		if !recursive {
			break MAIN_LOOP
		}
	}

	return result, nil
}

func readString(buf *bytes.Buffer) (string, error) {
	var strlen uint16
	err := binary.Read(buf, binary.BigEndian, &strlen)
	if err != nil {
		return "", err
	}
	strbytes := make([]byte, strlen)
	n, err := buf.Read(strbytes)
	if (err != nil) || (n != int(strlen)) {
		return "", errors.New("string underflow")
	}
	return string(strbytes), nil
}

// consume topic: __consumer_offsets and process the message to get offsets of consumers
func (this *ZkCluster) processConsumerOffsetsMessage(msg *sarama.ConsumerMessage) {
	var keyver, valver uint16
	var partition uint32
	var offset, timestamp uint64

	buf := bytes.NewBuffer(msg.Key)
	err := binary.Read(buf, binary.BigEndian, &keyver)
	if (err != nil) || ((keyver != 0) && (keyver != 1)) {
		log.Warn("Failed to decode %s:%v offset %v: keyver", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	group, err := readString(buf)
	if err != nil {
		log.Warn("Failed to decode %s:%v offset %v: group", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	topic, err := readString(buf)
	if err != nil {
		log.Warn("Failed to decode %s:%v offset %v: topic", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &partition)
	if err != nil {
		log.Warn("Failed to decode %s:%v offset %v: partition", msg.Topic, msg.Partition, msg.Offset)
		return
	}

	buf = bytes.NewBuffer(msg.Value)
	err = binary.Read(buf, binary.BigEndian, &valver)
	if (err != nil) || ((valver != 0) && (valver != 1)) {
		log.Warn("Failed to decode %s:%v offset %v: valver", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &offset)
	if err != nil {
		log.Warn("Failed to decode %s:%v offset %v: offset", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	_, err = readString(buf)
	if err != nil {
		log.Warn("Failed to decode %s:%v offset %v: metadata", msg.Topic, msg.Partition, msg.Offset)
		return
	}
	err = binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		log.Warn("Failed to decode %s:%v offset %v: timestamp", msg.Topic, msg.Partition, msg.Offset)
		return
	}

	partitionOffset := &PartitionOffset{
		Cluster:   this.Name(),
		Topic:     topic,
		Partition: int32(partition),
		Group:     group,
		Timestamp: int64(timestamp),
		Offset:    int64(offset),
	}
	log.Debug("%+v", partitionOffset)
	return
}
