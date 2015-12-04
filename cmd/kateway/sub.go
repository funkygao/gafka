package main

import (
	"net/http"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

// /{ver}/topics/{topic}/{group}/{id}?offset=n&limit=1
func (this *Gateway) subHandler(w http.ResponseWriter, r *http.Request) {
	writeKatewayHeader(w)

	if this.breaker.Open() {
		writeBreakerOpen(w)
		return
	}

	var (
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
	//ver := params["ver"] // TODO
	topic = params["topic"]
	group = params["group"]

	if !this.meta.AuthSub(r.Header.Get("Appid"), r.Header.Get("Subkey"), topic) {
		writeAuthFailure(w)
		log.Warn("consumer %s{topic:%s, group:%s, limit:%d} auth fail",
			r.RemoteAddr, topic, group, limit)

		return
	}

	log.Trace("consumer %s{topic:%s, group:%s, limit:%d}",
		r.RemoteAddr, topic, group, limit)

	// pick a consumer from the consumer group
	fetcher, err := this.subStore.Fetch(options.cluster, topic, group, r.RemoteAddr)
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
		err = this.consumeMulti(w, fetcher, limit)
	} else {
		err = this.consumeSingle(w, fetcher)
	}

	if err != nil {
		// broken pipe, io timeout
		log.Error("consumer %s{topic:%s, group:%s, limit:%d} get killed: %v",
			r.RemoteAddr, topic, group, limit, err)
		go this.subStore.KillClient(r.RemoteAddr) // wait cf.ProcessingTimeout
	}

}

func (this *Gateway) consumeSingle(w http.ResponseWriter, cg store.Fetcher) error {
	select {
	case msg := <-cg.Messages():
		// TODO when remote close silently, the write still ok
		// which will lead to msg losing for sub
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

func (this *Gateway) consumeMulti(w http.ResponseWriter, cg store.Fetcher, limit int) error {
	n := 0
	for {
		select {
		case msg := <-cg.Messages():
			if _, err := w.Write(msg.Value); err != nil {
				// TODO if cf.ChannelBufferSize > 0, client may lose message
				// got message in chan, client not recv it but offset commited.
				return err
			}

			// http chunked: len in hex
			// curl CURLOPT_HTTP_TRANSFER_DECODING will auto unchunk
			w.(http.Flusher).Flush()

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
