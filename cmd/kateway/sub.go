package main

import (
	"encoding/binary"
	"net/http"

	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
	"github.com/wvanbergen/kafka/consumergroup"
)

// /{ver}/topics/{topic}/{group}/{id}?offset=n&limit=1
func (this *Gateway) subHandler(w http.ResponseWriter, r *http.Request) {
	if this.breaker.Open() {
		writeBreakerOpen(w)
		return
	}

	var (
		ver   string
		topic string
		group string
		err   error
	)

	limit, err := getHttpQueryInt(r, "limit", 1)
	if err != nil {
		writeBadRequest(w)
		return
	}

	params := mux.Vars(r)
	ver = params["ver"]
	topic = params["topic"]
	group = params["group"]

	if !this.authSub(r.Header.Get("Subkey"), topic) {
		writeAuthFailure(w)
		log.Warn("consumer %s{topic:%s, group:%s, limit:%d} auth fail",
			r.RemoteAddr, topic, group, limit)

		// close the suspicous http connection  TODO test case
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Write([]byte("auth fail"))
		conn.Close()
		return
	}

	log.Trace("consumer %s{topic:%s, group:%s, limit:%d}",
		r.RemoteAddr, topic, group, limit)

	// pick a consumer from the consumer group
	cg, err := this.subPool.PickConsumerGroup(ver, topic, group, r.RemoteAddr)
	if err != nil {
		if isBrokerError(err) {
			// broker error
			this.breaker.Fail()
		}

		log.Error("consumer %s{topic:%s, group:%s, limit:%d} %v",
			r.RemoteAddr, topic, group, limit, err)

		writeBadRequest(w)
		if _, err = w.Write([]byte(err.Error())); err != nil {
			log.Error("consumer %s{topic:%s, group:%s, limit:%d} %v",
				r.RemoteAddr, topic, group, limit, err)
		}
		return
	}

	if limit > 1 {
		err = this.consumeMulti(w, cg, limit)
	} else {
		err = this.consumeSingle(w, cg)
	}

	if err != nil {
		// broken pipe, io timeout
		log.Error("consumer %s{topic:%s, group:%s, limit:%d} get killed: %v",
			r.RemoteAddr, topic, group, limit, err)
		go this.subPool.KillClient(topic, group, r.RemoteAddr) // wait cf.ProcessingTimeout
	}

}

func (this *Gateway) consumeSingle(w http.ResponseWriter, cg *consumergroup.ConsumerGroup) error {
	select {
	case msg := <-cg.Messages():
		if _, err := w.Write(msg.Value); err != nil {
			return err
		}

		// client really got this msg, safe to commit
		cg.CommitUpto(msg)

	case err := <-cg.Errors():
		return err
	}

	return nil
}

func (this *Gateway) consumeMulti(w http.ResponseWriter, cg *consumergroup.ConsumerGroup, limit int) error {
	flusher := w.(http.Flusher)
	n, lenbuf := 0, make([]byte, 4) // int32 is 4 bytes
	for {
		select {
		case msg := <-cg.Messages():
			binary.BigEndian.PutUint32(lenbuf, uint32(len(msg.Value)))
			// msg len
			if _, err := w.Write(lenbuf); err != nil {
				return err
			}
			// msg body
			if _, err := w.Write(msg.Value); err != nil {
				// TODO if cf.ChannelBufferSize > 0, client may lose message
				// got message in chan, client not recv it but offset commited.
				return err
			}

			// http chunked
			flusher.Flush()

			// client really got this msg, safe to commit
			cg.CommitUpto(msg)

			n++
			if n >= limit {
				return nil
			}

		case err := <-cg.Errors():
			return err
		}
	}

	return nil

}
