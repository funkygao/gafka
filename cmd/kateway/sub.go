package main

import (
	"net/http"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

// /topics/{ver}/{topic}/{group}?offset=n&limit=1
func (this *Gateway) subHandler(w http.ResponseWriter, r *http.Request) {
	writeKatewayHeader(w)

	if this.breaker.Open() {
		writeBreakerOpen(w)
		return
	}

	var (
		topic string
		ver   string
		appid string
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
	appid = r.Header.Get("Appid")

	if !this.meta.AuthSub(appid, r.Header.Get("Subkey"), topic) {
		writeAuthFailure(w)
		log.Warn("consumer %s{topic:%s, ver:%s, group:%s, limit:%d} auth fail",
			r.RemoteAddr, topic, ver, group, limit)

		return
	}

	log.Trace("consumer %s{topic:%s, ver:%s, group:%s, limit:%d}",
		r.RemoteAddr, topic, ver, group, limit)

	topic = kafkaTopic(appid, topic, ver)
	// pick a consumer from the consumer group
	fetcher, err := this.subStore.Fetch(options.cluster, topic, group, r.RemoteAddr)
	if err != nil {
		if isBrokerError(err) {
			// broker error
			this.breaker.Fail()
		}

		log.Error("consumer %s{topic:%s, ver:%s, group:%s, limit:%d} %v",
			r.RemoteAddr, topic, ver, group, limit, err)

		writeBadRequest(w)
		if _, err = w.Write([]byte(err.Error())); err != nil {
			log.Error("consumer %s{topic:%s, ver:%s, group:%s, limit:%d} %v",
				r.RemoteAddr, topic, ver, group, limit, err)
		}
		return
	}

	err = this.fetchMessages(w, fetcher, limit)
	if err != nil {
		// broken pipe, io timeout
		log.Error("consumer %s{topic:%s, ver:%s, group:%s, limit:%d} get killed: %v",
			r.RemoteAddr, topic, ver, group, limit, err)
		go this.subStore.KillClient(r.RemoteAddr) // wait cf.ProcessingTimeout
	}

}

func (this *Gateway) fetchMessages(w http.ResponseWriter, fetcher store.Fetcher, limit int) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	remoteCloseNotify := w.(http.CloseNotifier).CloseNotify()
	n := 0
	for {
		select {
		case <-remoteCloseNotify:
			return ErrRemoteInterrupt

		case msg := <-fetcher.Messages():
			// TODO when remote close silently, the write still ok
			// which will lead to msg losing for sub
			if _, err := w.Write(msg.Value); err != nil {
				// TODO if cf.ChannelBufferSize > 0, client may lose message
				// got message in chan, client not recv it but offset commited.
				return err
			}

			// client really got this msg, safe to commit
			log.Debug("commit offset: {T:%s, P:%d, O:%d}", msg.Topic, msg.Partition, msg.Offset)
			fetcher.CommitUpto(msg)

			n++
			if n >= limit {
				return nil
			}

			// http chunked: len in hex
			// curl CURLOPT_HTTP_TRANSFER_DECODING will auto unchunk
			w.(http.Flusher).Flush()

		case <-ticker.C:
			log.Debug("recv msg timeout")
			w.WriteHeader(http.StatusNoContent)
			// TODO write might fail, remote client might have died
			w.Write([]byte{}) // without this, client cant get response
			return nil

		case err := <-fetcher.Errors():
			return err

		}
	}

	return nil

}
