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
	req.Body = http.MaxBytesReader(w, req.Body, options.maxBodySize)
	err := req.ParseForm() // TODO
	if err != nil {
		log.Error("%s: %v", req.RemoteAddr, err)

		this.writeBadRequest(w)
		return
	}

	if !this.authenticate(req) {
		this.writeAuthFailure(w)
		return
	}

	if this.breaker.Open() {
		this.writeBreakerOpen(w)
		return
	}

	this.metrics.PubConcurrent.Inc(1)

	t1 := time.Now()
	var (
		ver   string
		topic string
	)

	params := mux.Vars(req)
	ver = params["ver"]
	topic = params["topic"]

	// TODO how can get m in []byte?
	partition, offset, err := this.produce(ver, topic, req.FormValue("m"))
	if err != nil {
		this.breaker.Fail()
		this.metrics.PubConcurrent.Dec(1)
		this.metrics.PubFailure.Inc(1)
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
		this.metrics.ClientError.Inc(1)
	}

	this.metrics.PubSuccess.Inc(1)
	this.metrics.PubConcurrent.Dec(1)
	this.metrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6)
}

func (this *Gateway) produce(ver, topic string, msg string) (partition int32,
	offset int64, err error) {
	this.metrics.PubQps.Mark(1)
	this.metrics.PubSize.Mark(int64(len(msg)))

	client, e := this.kpool.Get()
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

func (this *Gateway) produceWithoutPool(ver, topic string, msg string) (partition int32,
	offset int64, err error) {
	this.metrics.PubQps.Mark(1)
	this.metrics.PubSize.Mark(int64(len(msg)))

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
