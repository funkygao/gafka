package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// /topics/:appid/:topic/:ver/:group?limit=1&reset=newest
func (this *Gateway) subHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic    string
		ver      string
		myAppid  string
		hisAppid string
		reset    string
		group    string
		err      error
	)

	query := r.URL.Query()
	reset = query.Get(UrlQueryReset)
	limit, err := getHttpQueryInt(&query, "limit", 1)
	if err != nil {
		this.writeBadRequest(w, err)
		return
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)
	group = params.ByName(UrlParamGroup)
	if strings.Contains(group, ".") { // TODO more strict rules
		log.Warn("consumer %s{topic:%s, ver:%s, group:%s, limit:%d} invalid group",
			r.RemoteAddr, topic, ver, group, limit)

		http.Error(w, "group cannot contain dot", http.StatusBadRequest)
		return
	}

	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey), topic); err != nil {
		log.Error("consumer[%s] %s {hisapp:%s, topic:%s, ver:%s, group:%s, limit:%d}: %s",
			myAppid, r.RemoteAddr, hisAppid, topic, ver, group, limit, err)

		this.writeAuthFailure(w, err)
		return
	}

	log.Debug("sub[%s] %s: %+v", myAppid, r.RemoteAddr, params)

	rawTopic := meta.KafkaTopic(hisAppid, topic, ver)
	// pick a consumer from the consumer group
	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("cluster not found for subd app: %s", hisAppid)

		http.Error(w, "invalid subd appid", http.StatusBadRequest)
		return
	}

	fetcher, err := store.DefaultSubStore.Fetch(cluster, rawTopic,
		myAppid+"."+group, r.RemoteAddr, reset)
	if err != nil {
		log.Error("sub[%s] %s: %+v %v", myAppid, r.RemoteAddr, params, err)

		this.writeBadRequest(w, err)
		return
	}

	err = this.fetchMessages(w, fetcher, limit, myAppid, hisAppid, topic, ver)
	if err != nil {
		// e,g. broken pipe, io timeout, client gone
		log.Error("sub[%s] %s: %+v %v", myAppid, r.RemoteAddr, params, err)

		go store.DefaultSubStore.KillClient(r.RemoteAddr) // wait cf.ProcessingTimeout
	}

}

func (this *Gateway) fetchMessages(w http.ResponseWriter, fetcher store.Fetcher,
	limit int, myAppid, hisAppid, topic, ver string) error {
	clientGoneCh := w.(http.CloseNotifier).CloseNotify()

	chunkedBeforeTimeout := false
	chunkedEver := false
	n := 0
	for {
		select {
		case <-clientGoneCh:
			return ErrClientGone

		case <-this.shutdownCh:
			if !chunkedEver {
				w.WriteHeader(http.StatusNoContent)
				w.Write([]byte{})
			}
			return nil

		case msg := <-fetcher.Messages():
			// TODO when remote close silently, the write still ok
			// which will lead to msg losing for sub
			if _, err := w.Write(msg.Value); err != nil {
				// TODO if cf.ChannelBufferSize > 0, client may lose message
				// got message in chan, client not recv it but offset commited.
				return err
			}

			// client really got this msg, safe to commit
			// TODO test case: client got chunk 2, then killed. should server commit offset?
			log.Debug("commit offset: {T:%s, P:%d, O:%d}", msg.Topic, msg.Partition, msg.Offset)
			fetcher.CommitUpto(msg)

			this.subMetrics.ConsumeOk(myAppid, topic, ver)
			this.subMetrics.ConsumedOk(hisAppid, topic, ver)

			n++
			if n >= limit {
				return nil
			}

			// http chunked: len in hex
			// curl CURLOPT_HTTP_TRANSFER_DECODING will auto unchunk
			w.(http.Flusher).Flush()

			chunkedBeforeTimeout = true
			chunkedEver = true

		case <-this.timer.After(options.SubTimeout):
			if chunkedBeforeTimeout {
				log.Debug("await message timeout, chunked to next round")

				chunkedBeforeTimeout = false
				continue
			}

			if chunkedEver {
				// response already sent in chunk
				log.Debug("await message timeout, chunk finished")
				return nil
			}

			// never chunked, so send empty data
			log.Debug("await message timeout, writing empty data")
			w.WriteHeader(http.StatusNoContent)
			// TODO write might fail, remote client might have died
			w.Write([]byte{}) // without this, client cant get response
			return nil

		case err := <-fetcher.Errors():
			// e,g. consume a non-existent topic
			return err
		}
	}

}

// /raw/topics/:appid/:topic/:ver
// tells client how to sub in raw mode: how to connect kafka
func (this *Gateway) subRawHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic    string
		ver      string
		hisAppid string
		myAppid  string
	)

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey), topic); err != nil {
		log.Error("consumer[%s] %s {topic:%s, ver:%s, hisapp:%s}: %s",
			myAppid, r.RemoteAddr, topic, ver, hisAppid, err)

		this.writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("cluster not found for subd app: %s", hisAppid)

		http.Error(w, "invalid subd appid", http.StatusBadRequest)
		return
	}

	this.writeKatewayHeader(w)
	var out = map[string]string{
		"store": "kafka",
		"zk":    meta.Default.ZkCluster(cluster).ZkConnectAddr(),
		"topic": meta.KafkaTopic(hisAppid, topic, ver),
	}
	b, _ := json.Marshal(out)
	w.Header().Set(ContentTypeText, ContentTypeJson)
	w.Write(b)
}

// /ws/topics/:appid/:topic/:ver/:group
func (this *Gateway) subWsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		return
	}

	defer ws.Close()
}
