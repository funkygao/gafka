package main

import (
	"net/http"
	"strconv"

	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
	"github.com/wvanbergen/kafka/consumergroup"
)

// /{ver}/topics/{topic}/{group}/{id}?offset=n&limit=1
func (this *Gateway) subHandler(w http.ResponseWriter, req *http.Request) {
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
		group string
		err   error
		limit int = 1
	)

	limitParam := req.URL.Query().Get("limit")
	if limitParam != "" {
		limit, err = strconv.Atoi(limitParam)
		if err != nil {
			this.writeBadRequest(w)

			log.Error("consumer %s{topic:%s, group:%s, limit:%s} %v",
				req.RemoteAddr, topic, group, limitParam, err)
			return
		}
	}

	params := mux.Vars(req)
	ver = params["ver"]
	topic = params["topic"]
	group = params["group"]
	log.Info("consumer %s{topic:%s, group:%s, limit:%s}",
		req.RemoteAddr, topic, group, limitParam)

	// pick a consumer from the consumer group
	cg, err := this.subPool.PickConsumerGroup(topic, group, req.RemoteAddr)
	if err != nil {
		log.Error("consumer %s{topic:%s, group:%s, limit:%s} %v",
			req.RemoteAddr, topic, group, limitParam, err)

		this.writeBadRequest(w)
		w.Write([]byte(err.Error()))
		return
	}

	if err = this.consume(ver, topic, limit, group, w, req, cg); err != nil {
		log.Error("consumer %s{topic:%s, group:%s, limit:%s} get killed: %v",
			req.RemoteAddr, topic, group, limitParam, err)
		go this.subPool.KillClient(topic, group, req.RemoteAddr)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func (this *Gateway) consume(ver, topic string, limit int, group string,
	w http.ResponseWriter, req *http.Request, cg *consumergroup.ConsumerGroup) error {
	n := 0
	for {
		select {
		case msg := <-cg.Messages():
			if _, err := w.Write(msg.Value); err != nil {
				// if cf.ChannelBufferSize > 0, client may lose message
				// got message in chan, client not recv it but offset commited.
				return err
			}

			// client really got this msg, safe to commit
			cg.CommitUpto(msg)

			if limit > 0 {
				n++
				if n >= limit {
					return nil
				}
			}

		case err := <-cg.Errors():
			// TODO how to handle the errors
			log.Error("consumer %s{topic:%s, group:%s}: %+v", req.RemoteAddr, topic, group, err)
		}
	}

	return nil

}
