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
	if this.breaker.Open() {
		writeBreakerOpen(w)
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
			writeBadRequest(w)

			log.Error("consumer %s{topic:%s, group:%s, limit:%s} %v",
				req.RemoteAddr, topic, group, limitParam, err)
			return
		}
	}

	params := mux.Vars(req)
	ver = params["ver"]
	topic = params["topic"]
	group = params["group"]

	if !this.authSub(req.Header.Get("Subkey"), topic) {
		writeAuthFailure(w)
		return
	}

	log.Trace("consumer %s{topic:%s, group:%s, limit:%s}",
		req.RemoteAddr, topic, group, limitParam)

	// pick a consumer from the consumer group
	cg, err := this.subPool.PickConsumerGroup(topic, group, req.RemoteAddr)
	if err != nil {
		if isBreakeableError(err) {
			// broker error
			this.breaker.Fail()
		}

		log.Error("consumer %s{topic:%s, group:%s, limit:%s} %v",
			req.RemoteAddr, topic, group, limitParam, err)

		writeBadRequest(w)
		w.Write([]byte(err.Error()))
		return
	}

	if err = this.consume(ver, topic, limit, group, w, req, cg); err != nil {
		log.Error("consumer %s{topic:%s, group:%s, limit:%s} get killed: %v",
			req.RemoteAddr, topic, group, limitParam, err)
		go this.subPool.KillClient(topic, group, req.RemoteAddr) // wait cf.ProcessingTimeout

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

			// FIXME if client reached this limit and disconnects, leave the consumer garbage
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
