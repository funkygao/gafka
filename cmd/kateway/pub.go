package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

type pubResponse struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// /{ver}/topics/{topic}?key=xxx
func (this *Gateway) pubHandler(w http.ResponseWriter, r *http.Request) {
	if this.breaker.Open() {
		writeBreakerOpen(w)
		return
	}

	var (
		ver   string
		topic string
		key   string
	)

	params := mux.Vars(r)
	ver = params["ver"]
	topic = params["topic"]
	key = r.URL.Query().Get("key") // if key given, batched msg must belong to same key

	if !this.authPub(r.Header.Get("Pubkey"), topic) {
		writeAuthFailure(w)
		return
	}

	// get the raw POST message
	pr := io.LimitReader(r.Body, options.maxPubSize+1)
	rawMsg, err := ioutil.ReadAll(pr) // TODO optimize
	if err != nil {
		writeBadRequest(w)
		return
	}

	t1 := time.Now()
	this.pubMetrics.PubConcurrent.Inc(1)

	partition, offset, err := this.syncProduce(ver, topic, key, rawMsg)
	if err != nil {
		if isBrokerError(err) {
			this.breaker.Fail()
		}

		this.pubMetrics.PubConcurrent.Dec(1)
		this.pubMetrics.PubFailure.Inc(1)
		log.Error("%s: %v", r.RemoteAddr, err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := pubResponse{
		Partition: partition,
		Offset:    offset,
	}
	b, _ := json.Marshal(response)
	if _, err := w.Write(b); err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		this.pubMetrics.ClientError.Inc(1)
	}

	this.pubMetrics.PubSuccess.Inc(1)
	this.pubMetrics.PubConcurrent.Dec(1)
	this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
}

func (this *Gateway) asyncProduce(ver, topic string, key string, msg []byte) (err error) {
	client, e := this.pubPool.Get()
	if e != nil {
		if client != nil {
			client.Recycle()
		}
		return e
	}

	var producer sarama.AsyncProducer
	producer, err = sarama.NewAsyncProducerFromClient(client.Client)
	if err != nil {
		client.Recycle()
		return
	}

	// TODO pool up the error collector goroutines
	// messages will only be returned here after all retry attempts are exhausted.
	go func() {
		for err := range producer.Errors() {
			log.Error("async producer:", err)
		}
	}()

	var keyEncoder sarama.Encoder = nil // will use random partitioner
	if key != "" {
		keyEncoder = sarama.StringEncoder(key) // will use hash partition
		log.Debug("keyed message: %s", key)
	}
	producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	}
	return
}

func (this *Gateway) syncProduce(ver, topic string, key string, msg []byte) (partition int32,
	offset int64, err error) {
	this.pubMetrics.PubQps.Mark(1)
	this.pubMetrics.PubSize.Mark(int64(len(msg)))

	client, e := this.pubPool.Get()
	if e != nil {
		if client != nil {
			client.Recycle()
		}
		return -1, -1, e
	}

	var producer sarama.SyncProducer
	producer, err = sarama.NewSyncProducerFromClient(client.Client)
	if err != nil {
		client.Recycle()
		return
	}

	// TODO add msg header

	var keyEncoder sarama.Encoder = nil // will use random partitioner
	if key != "" {
		keyEncoder = sarama.StringEncoder(key) // will use hash partition
	}
	partition, offset, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoder,
		Value: sarama.ByteEncoder(msg),
	})

	producer.Close() // TODO keep the conn open
	client.Recycle()
	return
}

// Just for testing, a throughput of only 1k/s
func (this *Gateway) produceWithoutPool(ver, topic string, msg []byte) (partition int32,
	offset int64, err error) {
	this.pubMetrics.PubQps.Mark(1)
	this.pubMetrics.PubSize.Mark(int64(len(msg)))

	var producer sarama.SyncProducer
	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second
	//cf.Producer.Compression = sarama.CompressionSnappy
	cf.Producer.Retry.Max = 3
	producer, err = sarama.NewSyncProducer(this.metaStore.BrokerList(), cf)
	if err != nil {
		return -1, -1, err
	}

	partition, offset, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	})
	producer.Close()
	return
}
