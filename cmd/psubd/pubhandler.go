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

// /{ver}/topics/{topic}?ack=n&retry=n
func (this *Gateway) pubHandler(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, 1<<20) // TODO

	var (
		ver    string
		topic  string
		pubkey string
		ack    int = 1
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
	log.Debug("ver: %s ack: %d", ver, ack)

	pubkeyParam := req.Header["Pubkey"]
	if len(pubkeyParam) > 0 {
		pubkey = pubkeyParam[0]
	}

	log.Debug("pubkey: %s", pubkey)

	offset, err := this.singleSend(topic, req.FormValue("m"))
	log.Info("offset: %d err: %v", offset, err)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
}

func (this *Gateway) singleSend(topic string, msg string) (offset int64, err error) {
	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.WaitForLocal
	cf.Producer.Partitioner = sarama.NewHashPartitioner
	cf.Producer.Timeout = time.Second
	cf.Producer.Retry.Max = 3
	var producer sarama.SyncProducer
	log.Debug("kafka connecting")
	producer, err = sarama.NewSyncProducer([]string{"localhost:9092"}, cf)
	if err != nil {
		return
	}

	log.Debug("sending %s msg: %s", topic, msg)
	_, offset, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic:    topic,
		Value:    sarama.StringEncoder(msg),
		Metadata: "haha",
	})

	producer.Close()
	return
}
