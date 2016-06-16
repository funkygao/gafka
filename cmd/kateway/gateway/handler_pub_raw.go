// +build !fasthttp

package gateway

import (
	"bytes"
	"net/http"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// POST /v1/raw/msgs/:cluster/:topic?key=mykey&async=1&ack=all
func (this *pubServer) pubRawHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		cluster      string
		topic        string
		partitionKey string
		t1           = time.Now()
	)

	realIp := getHttpRemoteIp(r)
	topic = params.ByName(UrlParamTopic)
	cluster = params.ByName("cluster")

	buf := bytes.NewBuffer(make([]byte, 0, 1<<10))
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		log.Error("pub raw %s(%s) {C:%s T:%s UA:%s} %s",
			r.RemoteAddr, realIp, cluster, topic, r.Header.Get("User-Agent"), err)

		this.pubMetrics.ClientError.Inc(1)
		writeBadRequest(w, err.Error())
		return
	}

	body := buf.Bytes()

	if !Options.DisableMetrics {
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(body)))
	}

	query := r.URL.Query() // reuse the query will save 100ns
	partitionKey = query.Get("key")

	pubMethod := store.DefaultPubStore.SyncPub
	if query.Get("async") == "1" {
		pubMethod = store.DefaultPubStore.AsyncPub
	}
	if query.Get("ack") == "all" {
		pubMethod = store.DefaultPubStore.SyncAllPub
	}

	_, _, err = pubMethod(cluster, topic, []byte(partitionKey), body)
	if err != nil {
		log.Error("pub raw %s(%s) {C:%s T:%s UA:%s} %s",
			r.RemoteAddr, realIp, cluster, topic, r.Header.Get("User-Agent"), err)

		writeServerError(w, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)

	if _, err = w.Write(ResponseOk); err != nil {
		log.Error("pub raw %s(%s) {C:%s T:%s UA:%s} %s",
			r.RemoteAddr, realIp, cluster, topic, r.Header.Get("User-Agent"), err)

		this.pubMetrics.ClientError.Inc(1)
	}

	if !Options.DisableMetrics {
		this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
	}

}
