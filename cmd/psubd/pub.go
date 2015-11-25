package main

import (
	//"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

// /{ver}/topics/{topic}?ack=n&retry=n&timeout=n
func (this *Gateway) pubHandler(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<20) // TODO

	// authentication TODO
	pubkeyParam := req.Header["Pubkey"]
	if len(pubkeyParam) > 0 && !this.metaStore.AuthPub(pubkeyParam[0]) {
		log.Debug("pubkey: %s", pubkeyParam[0])
		this.writeAuthFailure(w)
		return
	}

	var (
		ver   string
		topic string
		ack   int = 1
	)
	req.ParseForm()

	// kafka ack
	ackParam := req.FormValue("ack")
	if ackParam != "" {
		ack, _ = strconv.Atoi(ackParam)
	}

	vars := mux.Vars(req)
	ver = vars["ver"]
	topic = vars["topic"]
	log.Debug("ver:%s topic:%s ack:%d", ver, topic, ack)

	offset, err := this.doSendMessage(topic, req.FormValue("m"))
	log.Debug("offset: %d err: %v", offset, err)

	w.Header().Set("Content-Type", "html/text")
	w.WriteHeader(http.StatusOK)
}

func (this *Gateway) doSendMessage(topic string, msg string) (offset int64, err error) {
	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second
	cf.Producer.Compression = sarama.CompressionSnappy
	cf.Producer.Retry.Max = 3
	var producer sarama.SyncProducer
	log.Debug("kafka connecting")
	producer, err = sarama.NewSyncProducer([]string{"localhost:9092"}, cf)
	if err != nil {
		return
	}

	// add msg header

	log.Debug("sending %s msg: %s", topic, msg)
	_, offset, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic:    topic,
		Value:    sarama.StringEncoder(msg),
		Metadata: "haha",
	})

	producer.Close()
	return
}
