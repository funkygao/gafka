package monitor

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type KafkaMessage struct {
	Offset int64  `json:"offset"`
	Key    []byte `json:"key"`
	Value  []byte `json:"value"` // json.Marshal([]byte) encodes as a base64-encoded string
}

type KafkaMessages struct {
	Cluster   string          `json:"cluster"`
	Topic     string          `json:"topic"`
	Partition int32           `json:"partition"`
	Messages  []*KafkaMessage `json:"messages"`
}

// @rest GET /kfk/peek/:cluster/:topic/:partition/:offset/:count&wait=5s
// peek topic/partition's message(s)
// eg: curl -XGET http://192.168.149.150:10025/kfk/peek/bigtopic_cluster_02/cluster_02_topic_01/0/10/1
// TODO authz and rate limitation
func (this *Monitor) kfkPeekHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {

	defer func() {
		if err := recover(); err != nil {
			log.Error("%+v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}()

	var (
		cluster   string
		topic     string
		partition int32
		offset    int64
		count     int
		wait      time.Duration
	)

	cluster = params.ByName(UrlParamCluster)
	topic = params.ByName(UrlParamTopic)
	p := params.ByName(UrlParamPartition)
	o := params.ByName(UrlParamOffset)
	c := params.ByName(UrlParamCount)

	pid, err := strconv.Atoi(p)
	if err != nil || pid < 0 {
		writeBadRequest(w, "invalid partition")
		return
	}
	partition = int32(pid)

	offset, err = strconv.ParseInt(o, 10, 64)
	if err != nil || offset < 0 {
		writeBadRequest(w, "invalid offset")
		return
	}

	count, err = strconv.Atoi(c)
	if err != nil || count < 0 {
		writeBadRequest(w, "invalid count")
		return
	}

	q := r.URL.Query()
	waitParam := q.Get("wait")
	defaultWait := time.Second * 2
	wait = defaultWait
	if waitParam != "" {
		wait, _ = time.ParseDuration(waitParam)
		if wait.Seconds() < 1 {
			wait = defaultWait
		} else if wait.Seconds() > 5. {
			wait = defaultWait
		}
	}

	// max count will be 100
	if count > 100 {
		count = 100
	}

	// check cluster
	allClusters := this.zkzone.Clusters()
	if _, ok := allClusters[cluster]; !ok {
		writeBadRequest(w, ErrClusterNotExist.Error())
		return
	}

	zkCluster := this.zkzone.NewCluster(cluster)

	// check topic
	allTopics := zkCluster.TopicsCtime()
	if _, ok := allTopics[topic]; !ok {
		writeBadRequest(w, ErrTopicNotExist.Error())
		return
	}

	// create kfk client
	cf := sarama.NewConfig()
	cf.Consumer.Return.Errors = true // return err from Errors channel
	kfk, err := sarama.NewClient(zkCluster.BrokerList(), cf)
	if err != nil {
		writeServerError(w, err.Error())
		return
	}
	defer kfk.Close()

	// check partition
	allPartitions, err := kfk.Partitions(topic)
	if err != nil {
		writeServerError(w, err.Error())
		return
	}

	partitionExist := false
	for _, p := range allPartitions {
		if p == partition {
			partitionExist = true
			break
		}
	}

	if !partitionExist {
		writeBadRequest(w, ErrPartitionNotExist.Error())
		return
	}

	// check offset
	oldest, latest, err := this.getPartitionOffset(kfk, topic, partition)
	if err != nil {
		writeServerError(w, err.Error())
		return
	}

	if offset < oldest || offset >= latest {
		writeServerError(w, ErrOffsetOutOfRange.Error())
		return
	}

	maxCount := latest - oldest
	if int64(count) > maxCount {
		count = int(maxCount)
	}

	msgChan := make(chan *sarama.ConsumerMessage, 10)
	errs := make(chan error, 5)
	stopCh := make(chan struct{})
	wg := sync.WaitGroup{}

	// try to get message asynchronously
	wg.Add(1)
	go func() {
		defer wg.Done()

		msgCnt := 0
		consumer, err := sarama.NewConsumerFromClient(kfk)
		if err != nil {
			select {
			case errs <- err:
				return
			case <-stopCh:
				return
			}
		}
		defer consumer.Close()

		p, err := consumer.ConsumePartition(topic, partition, offset)
		if err != nil {
			select {
			case errs <- err:
				return
			case <-stopCh:
				return
			}
		}
		defer p.Close()

		for {
			select {
			case msg := <-p.Messages():
				msgChan <- msg
				msgCnt++
				if msgCnt >= count {
					return // enough
				}
			case err := <-p.Errors():
				if err != nil {
					select {
					case errs <- err:
						return
					case <-stopCh:
						return
					}
				}
			case <-stopCh:
				return
			}
		}

	}()

	// collect the result
	var kfkMessages KafkaMessages
	kfkMessages.Cluster = cluster
	kfkMessages.Topic = topic
	kfkMessages.Partition = partition
	n := 0
LOOP:
	for {
		select {
		case <-time.After(wait):
			break LOOP

		case err = <-errs:
			break LOOP

		case msg := <-msgChan:
			kfkMsg := &KafkaMessage{
				Offset: msg.Offset,
				Key:    msg.Key,
				Value:  msg.Value,
			}
			kfkMessages.Messages = append(kfkMessages.Messages, kfkMsg)

			n++
			if n >= count {
				break LOOP //enough
			}
		}
	}

	close(stopCh) // stop all the sub-goroutines
	wg.Wait()

	if err != nil {
		writeServerError(w, err.Error())
		return
	}

	b, _ := json.Marshal(kfkMessages)
	w.Write(b)

	return
}
