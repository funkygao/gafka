package monitor

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
	"net/http"
	"sort"
	"time"
)

type Partition struct {
	PartitionId int32   `json:"id"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replica"`
	ISR         []int   `json:"isr"`
	Oldest      int64   `json:"oldest"`
	Latest      int64   `json:"latest"`
	ErrMsg      string  `json:"error"`
}

type PartitionOrderById []*Partition

func (this PartitionOrderById) Len() int           { return len(this) }
func (this PartitionOrderById) Less(i, j int) bool { return this[i].PartitionId < this[j].PartitionId }
func (this PartitionOrderById) Swap(i, j int)      { this[i], this[j] = this[j], this[i] }

type Topic struct {
	Topic      string       `json:"topic"`
	Partitions []*Partition `json:"partitions"`
	ErrMsg     string       `json:"error"`
}

type ClusterTopics struct {
	Name   string   `json:"cluster_name"`
	Topics []*Topic `json:"topics"`
	ErrMsg string   `json:"error"`
}

type Topics struct {
	All []*ClusterTopics `json:"all_topics"`
}

// @rest POST /kfk/topics
// 1. how many clusters
// 2. how many topics in each cluster
// 3. how many partition for topic
// 4. each partition's leader broker id
// 5. each partition's replica broker ids
// 6. each partition's isr broker ids
// 7. each partition's offset range [oldest, latest)
// eg: curl -XPOST http://192.168.149.150:10025/kfk/topics -d'[{"cluster":"bigtopic", "topics":["182.KatewayTestTopic_02.v1"]}]'
// TODO authz and rate limitation
func (this *Monitor) kfkTopicsHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

	defer func() {
		if err := recover(); err != nil {
			log.Error("%+v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()

	type topicsRequestItem struct {
		Cluster string   `json:"cluster"`
		Topics  []string `json:"topics"`
	}

	type topicsRequest []topicsRequestItem

	dec := json.NewDecoder(r.Body)
	var req topicsRequest
	err := dec.Decode(&req)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	reqInfo := make(map[string][]string) // cluster --> topic list --> ["topic_01", "topic_02"]
	for _, c := range req {
		topics := make([]string, 0, len(c.Topics))

		for _, t := range c.Topics {
			topics = append(topics, t)
		}

		reqInfo[c.Cluster] = topics
	}

	topics, err := this.getTopics(reqInfo)
	if err != nil {
		log.Error("topics:%v %v", reqInfo, err)
		writeServerError(w, err.Error())
		return
	}

	b, _ := json.Marshal(topics)
	w.Write(b)

	return
}

func (this *Monitor) getTopics(reqClusters map[string][]string) (topics Topics, err error) {

	allClusters := this.zkzone.Clusters()

	// preallocate all slot
	if len(reqClusters) == 0 {
		// we need to get all clusters
		for cn, _ := range allClusters {
			ct := ClusterTopics{
				Name: cn,
			}

			topics.All = append(topics.All, &ct)
		}
	} else {
		// we need to get target clusters
		for reqC, _ := range reqClusters {
			ct := ClusterTopics{
				Name: reqC,
			}
			// check cluster exist or not
			if _, ok := allClusters[reqC]; !ok {
				ct.ErrMsg = ErrClusterNotExist.Error()
			}

			topics.All = append(topics.All, &ct)
		}
	}

	for _, ct := range topics.All {
		if ct.ErrMsg == "" { // valid cluster
			reqTopics, _ := reqClusters[ct.Name]
			err = this.getClusterTopics(ct, reqTopics)
			if err != nil {
				return
			}
		}
	}

	return
}

func (this *Monitor) getClusterTopics(ct *ClusterTopics, reqTopics []string) (err error) {

	zkCluster := this.zkzone.NewCluster(ct.Name)

	allTopics, err := zkCluster.Topics()
	if err != nil {
		return
	}

	var null struct{}
	allTopicsMap := make(map[string]struct{})
	for _, t := range allTopics {
		allTopicsMap[t] = null
	}

	if len(reqTopics) == 0 {
		// all topics info
		for tn, _ := range allTopicsMap {
			t := Topic{
				Topic: tn,
			}
			ct.Topics = append(ct.Topics, &t)
		}
	} else {
		// we need to get target topic
		for _, reqT := range reqTopics {
			t := Topic{
				Topic: reqT,
			}
			// check topic exist or not
			if _, ok := allTopicsMap[reqT]; !ok {
				t.ErrMsg = ErrTopicNotExist.Error()
			}

			ct.Topics = append(ct.Topics, &t)
		}
	}

	for _, t := range ct.Topics {
		if t.ErrMsg == "" { // valid topic
			err = this.getClusterTopic(zkCluster, t)
			if err != nil {
				return
			}
		}
	}

	return
}

func saramaConfig() *sarama.Config {
	cf := sarama.NewConfig()
	cf.Net.DialTimeout = time.Second * 4
	cf.Net.ReadTimeout = time.Second * 4
	cf.Net.WriteTimeout = time.Second * 4
	cf.Metadata.Retry.Max = 2
	cf.Producer.Timeout = time.Second * 4
	return cf
}

func (this *Monitor) getClusterTopic(zkCluster *gzk.ZkCluster, t *Topic) (err error) {

	kfkClient, err := sarama.NewClient(zkCluster.BrokerList(), saramaConfig())
	if err != nil {
		return
	}
	defer kfkClient.Close()

	allPartitions, err := kfkClient.Partitions(t.Topic)
	for _, pid := range allPartitions {
		p := Partition{
			PartitionId: pid,
		}

		err = this.getTopicPartition(zkCluster, kfkClient, &p, t.Topic)
		if err != nil {
			return
		}

		t.Partitions = append(t.Partitions, &p)
	}

	// sort by id
	sort.Sort(PartitionOrderById(t.Partitions))

	return
}

func (this *Monitor) getTopicPartition(zkCluster *gzk.ZkCluster,
	kfkClient sarama.Client,
	p *Partition,
	topic string) (err error) {

	// get leader
	p.Leader, err = this.getPartitionLeader(kfkClient, topic, p.PartitionId)
	if err != nil {
		return
	}

	// get replica
	p.Replicas, err = this.getPartitionReplica(kfkClient, topic, p.PartitionId)
	if err != nil {
		return
	}

	// get isr
	p.ISR, err = this.getPartitionIsr(zkCluster, topic, p.PartitionId)
	if err != nil {
		return
	}

	// get oldest/latest offset
	p.Oldest, p.Latest, err = this.getPartitionOffset(kfkClient, topic, p.PartitionId)
	if err != nil {
		return
	}

	return
}

func (this *Monitor) getPartitionLeader(kfkClient sarama.Client,
	topic string,
	partition int32) (leader int32, err error) {

	leaderBroker, err := kfkClient.Leader(topic, partition)
	if err != nil {
		return
	}

	leader = leaderBroker.ID()

	return
}

func (this *Monitor) getPartitionReplica(kfkClient sarama.Client,
	topic string,
	partition int32) (replicas []int32, err error) {

	replicas, err = kfkClient.Replicas(topic, partition)
	if err != nil {
		return
	}

	return
}

func (this *Monitor) getPartitionIsr(zkCluster *gzk.ZkCluster,
	topic string,
	partition int32) (isr []int, err error) {

	isr, _, _ = zkCluster.Isr(topic, partition)

	return
}

func (this *Monitor) getPartitionOffset(kfkClient sarama.Client,
	topic string,
	partition int32) (oldest int64, latest int64, err error) {

	oldest, err = kfkClient.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return
	}

	latest, err = kfkClient.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return
	}

	return
}
