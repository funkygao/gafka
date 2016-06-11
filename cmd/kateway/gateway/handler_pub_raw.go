// +build !fasthttp

package gateway

import (
	"io"
	"net/http"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/mpool"
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
	msgLen := int(r.ContentLength)

	var msg *mpool.Message
	msg = mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]

	// get the raw POST message
	lbr := io.LimitReader(r.Body, Options.MaxPubSize+1)
	if _, err := io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		log.Error("pub raw %s(%s) {C:%s T:%s UA:%s} %s",
			r.RemoteAddr, realIp, cluster, topic, r.Header.Get("User-Agent"), err)

		this.pubMetrics.ClientError.Inc(1)
		writeBadRequest(w, ErrTooBigMessage.Error())
		return
	}

	if !Options.DisableMetrics {
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(msg.Body)))
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

	_, _, err := pubMethod(cluster, topic, []byte(partitionKey), msg.Body)
	if err != nil {
		log.Error("pub raw %s(%s) {C:%s T:%s UA:%s} %s",
			r.RemoteAddr, realIp, cluster, topic, r.Header.Get("User-Agent"), err)

		msg.Free() // defer is costly

		writeServerError(w, err.Error())
		return
	}

	msg.Free()
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
