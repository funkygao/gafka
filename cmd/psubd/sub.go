package main

import (
	"net/http"
	"strconv"

	"github.com/Shopify/sarama"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

// /{ver}/topics/{topic}?offset=n&limit=n&timeout=n
func (this *Gateway) subHandler(w http.ResponseWriter, req *http.Request) {
	req.Body = http.MaxBytesReader(w, req.Body, options.maxBodySize)
	err := req.ParseForm()
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

	var (
		ver   string
		topic string
		limit int = 100
	)

	limitParam := req.FormValue("limit")
	if limitParam != "" {
		limit, err = strconv.Atoi(limitParam)
		if err != nil {
			this.writeBadRequest(w)

			log.Error("limit %s: %v", limitParam, err)
			return
		}
	}

	params := mux.Vars(req)
	ver = params["ver"]
	topic = params["topic"]
	log.Debug("ver:%s topic:%s limit:%d", ver, topic, limit)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	this.consume(ver, topic, w, req)
}

func (this *Gateway) consume(ver, topic string, w http.ResponseWriter,
	req *http.Request) {
	client, err := this.kpool.Get()
	if client != nil {
		defer client.Recycle()
	}
	if err != nil {
		log.Error(err)
	}

	consumer, err := sarama.NewConsumerFromClient(client.Client)
	if err != nil {
		this.breaker.Fail()

		log.Error(err)
		return
	}

	p, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		this.breaker.Fail()

		log.Error("%s: %v", req.RemoteAddr, err)
		return
	}

	var msg *sarama.ConsumerMessage
	for {
		select {
		case msg = <-p.Messages():
			_, err = w.Write(msg.Value)
			if err != nil {
				log.Error("%s: %v", req.RemoteAddr, err)
				break
			}

			w.Write([]byte("\n"))

		}
	}

}
