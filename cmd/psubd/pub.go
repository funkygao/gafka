package main

import (
	"encoding/json"
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

// /{ver}/topics/{topic}?ack=n&retry=n&timeout=n
func (this *Gateway) pubHandler(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, options.maxPubSize)
	err := req.ParseForm() // TODO
	if err != nil {
		log.Error("%s: %v", req.RemoteAddr, err)

		writeBadRequest(w)
		return
	}

	if this.breaker.Open() {
		writeBreakerOpen(w)
		return
	}

	var (
		ver   string
		topic string
	)

	params := mux.Vars(req)
	ver = params["ver"]
	topic = params["topic"]

	if !this.authPub(req.Header.Get("Pubkey"), topic) {
		writeAuthFailure(w)
		return
	}

	t1 := time.Now()
	this.pubMetrics.PubConcurrent.Inc(1)

	// TODO how can get m in []byte?
	partition, offset, err := this.produce(ver, topic, req.FormValue("m"))
	if err != nil {
		this.breaker.Fail()

		this.pubMetrics.PubConcurrent.Dec(1)
		this.pubMetrics.PubFailure.Inc(1)
		log.Error("%s: %v", req.RemoteAddr, err)

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
		log.Error("%s: %v", req.RemoteAddr, err)
		this.pubMetrics.ClientError.Inc(1)
	}

	this.pubMetrics.PubSuccess.Inc(1)
	this.pubMetrics.PubConcurrent.Dec(1)
	this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
}

func (this *Gateway) produce(ver, topic string, msg string) (partition int32,
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

	partition, offset, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg), // TODO
	})

	producer.Close() // TODO keep the conn open
	client.Recycle()
	return
}

// Deprecated, a throughput of only 1k/s
func (this *Gateway) produceWithoutPool(ver, topic string, msg string) (partition int32,
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
		Value: sarama.StringEncoder(msg),
	})
	producer.Close()
	return
}
