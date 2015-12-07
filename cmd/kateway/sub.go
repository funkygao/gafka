package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

// /topics/{appid}/{topic}/{ver}/_raw_
// tells client how to sub in raw mode: how to connect kafka
func (this *Gateway) subRawHandler(w http.ResponseWriter, r *http.Request) {
	var (
		topic string
		ver   string
		appid string
	)

	params := mux.Vars(r)
	ver = params["ver"]
	topic = params["topic"]
	appid = r.Header.Get("Appid")

	if !meta.Default.AuthSub(appid, r.Header.Get("Subkey"), topic) {

		this.writeAuthFailure(w)
		return
	}

	this.writeKatewayHeader(w)
	var out = map[string]string{
		"store": "kafka",
		"zk":    meta.Default.ZkCluster().ZkConnectAddr(),
		"topic": meta.KafkaTopic(appid, topic, ver),
	}
	b, _ := json.Marshal(out)
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

// /topics/{appid}/{topic}/{ver}/{group}?limit=1&reset=newest
func (this *Gateway) subHandler(w http.ResponseWriter, r *http.Request) {
	if this.breaker.Open() {
		this.writeBreakerOpen(w)
		return
	}

	var (
		topic    string
		ver      string
		myAppid  string
		hisAppid string
		reset    string
		group    string
		err      error
	)

	limit, err := getHttpQueryInt(r, "limit", 1)
	if err != nil {
		this.writeBadRequest(w, err)
		return
	}

	params := mux.Vars(r)
	ver = params["ver"]
	topic = params["topic"]
	group = params["group"]
	reset = params["reset"]
	hisAppid = params["appid"]
	myAppid = r.Header.Get("Appid")

	if !meta.Default.AuthSub(myAppid, r.Header.Get("Subkey"), topic) {
		log.Warn("consumer %s{topic:%s, ver:%s, group:%s, limit:%d} auth fail",
			r.RemoteAddr, topic, ver, group, limit)

		this.writeAuthFailure(w)
		return
	}

	log.Trace("sub[%s] %s: %+v", myAppid, r.RemoteAddr, params)

	rawTopic := meta.KafkaTopic(hisAppid, topic, ver)
	// pick a consumer from the consumer group
	fetcher, err := store.DefaultSubStore.Fetch(options.cluster, rawTopic, group, r.RemoteAddr, reset)
	if err != nil {
		if isBrokerError(err) {
			// broker error
			this.breaker.Fail()
		}

		log.Error("sub[%s] %s: %+v %v", myAppid, r.RemoteAddr, params, err)

		this.writeBadRequest(w, err)
		return
	}

	err = this.fetchMessages(w, fetcher, limit)
	if err != nil {
		// broken pipe, io timeout
		log.Error("sub[%s] %s: %+v %v", myAppid, r.RemoteAddr, params, err)

		go store.DefaultSubStore.KillClient(r.RemoteAddr) // wait cf.ProcessingTimeout
	}

}

func (this *Gateway) fetchMessages(w http.ResponseWriter, fetcher store.Fetcher, limit int) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	clientGoneCh := w.(http.CloseNotifier).CloseNotify()
	n := 0
	for {
		select {
		case <-clientGoneCh:
			return ErrClientGone

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
			log.Debug("recv msg timeout, writing empty data")
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
