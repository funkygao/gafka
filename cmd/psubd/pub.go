package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

// /{ver}/topics/{topic}?ack=n&retry=n&timeout=n
func (this *Gateway) pubHandler(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, options.maxBodySize)

	if !this.authenticate(req) {
		this.writeAuthFailure(w)
		return
	}

	if this.breaker.Open() {
		this.writeBreakerOpen(w)
		return
	}

	t1 := time.Now()
	var (
		ver   string
		topic string
		ack   int = 1
		err   error
	)
	req.ParseForm()

	// kafka ack
	ackParam := req.FormValue("ack")
	if ackParam != "" {
		ack, err = strconv.Atoi(ackParam)
		if err != nil {
			this.writeBadRequest(w)

			log.Error("ack %s: %v", ackParam, err)
			return
		}
	}

	vars := mux.Vars(req)
	ver = vars["ver"]
	topic = vars["topic"]
	log.Debug("ver:%s topic:%s ack:%d", ver, topic, ack)

	offset, err := this.doSendMessage(ver, topic, req.FormValue("m"))
	log.Debug("offset: %d err: %v", offset, err)

	w.Header().Set("Content-Type", "html/text")
	w.WriteHeader(http.StatusOK)

	this.metrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6)
}

func (this *Gateway) doSendMessage(ver, topic string, msg string) (offset int64, err error) {
	this.metrics.PubQps.Mark(1)
	this.metrics.PubSize.Mark(int64(len(msg)))

	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second
	//cf.Producer.Compression = sarama.CompressionSnappy
	cf.Producer.Retry.Max = 3
	var producer sarama.SyncProducer

	client, e := this.kpool.Get()
	if client != nil {
		defer client.Recycle()
	}
	if e != nil {
		log.Error(e)
		return -1, e
	}

	producer, err = sarama.NewSyncProducerFromClient(client.Client)
	if err != nil {
		this.breaker.Fail()
		return
	}

	// TODO add msg header

	log.Debug("sending %s msg: %s", topic, msg)
	_, offset, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic:    topic,
		Value:    sarama.StringEncoder(msg),
		Metadata: "haha",
	})
	if err != nil {
		this.breaker.Fail()
	}

	producer.Close() // TODO keep the conn open
	return
}
