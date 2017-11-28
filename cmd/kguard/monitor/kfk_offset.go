package monitor

import (
	"github.com/Shopify/sarama"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
	"net/http"
	"strconv"
)

// @rest PUT /kfk/offset/:cluster/:topic/:group/:partition?offset=xx
// reset group's offset on target cluster/topic/partition
// eg: curl -XPUT http://192.168.149.150:10025/kfk/offset/bigtopic_cluster_02/cluster_02_topic_01/my_cluster_02_cg_01/0?offset=12
// TODO authz and rate limitation
func (this *Monitor) resetOffsetHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		cluster   string
		topic     string
		partition string
		pid       int32
		group     string
		offset    string
		offsetN   int64
		err       error
	)

	cluster = params.ByName("cluster")
	topic = params.ByName("topic")
	group = params.ByName("group")
	partition = params.ByName("partition")
	offset = r.URL.Query().Get("offset")

	p, err := strconv.ParseInt(partition, 10, 32)
	if err != nil {
		log.Error("group reset offset {cluster:%s topic:%s partition:%s group:%s offset:%s} %v",
			cluster, topic, partition, group, offset, err)

		writeBadRequest(w, err.Error())
		return
	}
	pid = int32(p)

	offsetN, err = strconv.ParseInt(offset, 10, 64)
	if err != nil {
		log.Error("group reset offset {cluster:%s topic:%s partition:%s group:%s offset:%s} %v",
			cluster, topic, partition, group, offset, err)

		writeBadRequest(w, err.Error())
		return
	}

	if offsetN < 0 {
		log.Error("group reset offset {cluster:%s topic:%s partition:%s group:%s offset:%s} negative offset",
			cluster, topic, partition, group, offset)

		writeBadRequest(w, "offset must be positive")
		return
	}

	// check cluster
	allClusters := this.zkzone.Clusters()
	if _, ok := allClusters[cluster]; !ok {
		log.Error("group reset offset {cluster:%s topic:%s partition:%s group:%s offset:%s} cluster not exist",
			cluster, topic, partition, group, offset)

		writeBadRequest(w, ErrClusterNotExist.Error())
		return
	}

	// check topic
	zkCluster := this.zkzone.NewCluster(cluster)
	allTopics := zkCluster.TopicsCtime()
	if _, ok := allTopics[topic]; !ok {
		log.Error("group reset offset {cluster:%s topic:%s partition:%s group:%s offset:%s} topic not exist",
			cluster, topic, partition, group, offset)

		writeBadRequest(w, ErrTopicNotExist.Error())
		return
	}

	kfkClient, err := sarama.NewClient(zkCluster.BrokerList(), saramaConfig())
	if err != nil {
		return
	}
	defer kfkClient.Close()

	// check partition
	partitionExist := false
	allPartitions, err := kfkClient.Partitions(topic)
	for _, id := range allPartitions {
		if id == pid {
			partitionExist = true
			break
		}
	}

	if !partitionExist {
		log.Error("group reset offset {cluster:%s topic:%s partition:%s group:%s offset:%s} partition not exist",
			cluster, topic, partition, group, offset)

		writeBadRequest(w, ErrPartitionNotExist.Error())
		return
	}

	// check offset range
	oldest, latest, err := this.getPartitionOffset(kfkClient, topic, pid)
	if err != nil {
		return
	}

	if offsetN < oldest || offsetN > latest {
		log.Error("group reset offset {cluster:%s topic:%s partition:%s group:%s offset:%s} offset out of range",
			cluster, topic, partition, group, offset)

		writeBadRequest(w, ErrOffsetOutOfRange.Error())
		return
	}

	// reset offset
	err = zkCluster.ResetConsumerGroupOffset(topic, group, partition, offsetN)
	if err != nil {
		log.Error("group reset offset {cluster:%s topic:%s partition:%s group:%s offset:%s} %v",
			cluster, topic, partition, group, offset, err)

		writeServerError(w, err.Error())
		return
	}

	w.Write(ResponseOk)

	return
}
