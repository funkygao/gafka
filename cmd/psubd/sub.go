package main

import (
	"net/http"
	"strconv"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

// /{ver}/topics/{topic}/{group}/{id}?offset=n&limit=1&timeout=10m
// TODO offset manager, flusher, partitions, join group
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
		ver     string
		topic   string
		group   string
		client  string
		timeout time.Duration = time.Duration(time.Hour)
		err     error
		limit   int = 1
	)

	limitParam := req.URL.Query().Get("limit")
	timeoutParam := req.URL.Query().Get("timeout")
	if limitParam != "" {
		limit, err = strconv.Atoi(limitParam)
		if err != nil {
			this.writeBadRequest(w)

			log.Error("client[%s] from %s Sub {topic:%s, group:%s, limit:%s, timeout:%s} %v",
				client, req.RemoteAddr, topic, group, limitParam, timeoutParam, err)
			return
		}
	}
	if timeoutParam != "" {
		timeout, err = time.ParseDuration(timeoutParam)
		if err != nil {
			this.writeBadRequest(w)

			log.Error("client[%s] from %s Sub {topic:%s, group:%s, limit:%s, timeout:%s} %v",
				client, req.RemoteAddr, topic, group, limitParam, timeoutParam, err)
			return
		}

		if timeout.Nanoseconds() == 0 {
			timeout = time.Duration(time.Hour * 24 * 3650) // TODO 10 years is enough?
		}
	}

	params := mux.Vars(req)
	ver = params["ver"]
	topic = params["topic"]
	group = params["group"]
	client = params["id"]
	log.Info("client[%s] from %s Sub {topic:%s, group:%s, limit:%s, timeout:%s}",
		client, req.RemoteAddr, topic, group, limitParam, timeoutParam)

	if err = this.consume(ver, topic, limit, group, client, timeout, w, req); err != nil {
		this.breaker.Fail()
		log.Error("client[%s] from %s Sub {topic:%s, group:%s, limit:%s, timeout:%s} %v",
			client, req.RemoteAddr, topic, group, limitParam, timeoutParam, err)

		w.WriteHeader(http.StatusInternalServerError) // TODO
		w.Write([]byte(err.Error()))
	}
}

func (this *Gateway) consume(ver, topic string, limit int, group, client string,
	timeout time.Duration,
	w http.ResponseWriter, req *http.Request) error {
	cg, err := this.subPool.PickConsumerGroup(topic, group, client)
	if err != nil {
		return err
	}

	n := 0
	for {
		select {
		case <-time.After(timeout):
			return nil

		case msg := <-cg.Messages():
			if _, err := w.Write(msg.Value); err != nil {
				return err
			}
			this.subPool.TrackOffset(topic, group, client, msg)

			if limit > 0 {
				n++
				if n >= limit {
					return nil
				}
			}

		case err := <-cg.Errors():
			log.Error("%s %s %s: %+v", topic, group, client, err)
		}
	}

	return nil

}
