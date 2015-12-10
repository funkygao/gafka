package main

import (
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

type pubResponse struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

// /topics/:topic/:ver?key=mykey&async=1
func (this *Gateway) pubHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	if options.enableBreaker && this.breaker.Open() {
		this.writeBreakerOpen(w)
		return
	}

	if options.ratelimit && !this.leakyBuckets.Pour(r.RemoteAddr, 1) {
		this.writeQuotaExceeded(w)
		return
	}

	topic := params.ByName(UrlParamTopic)
	appid := r.Header.Get(HttpHeaderAppid)
	if !meta.Default.AuthPub(appid, r.Header.Get(HttpHeaderPubkey), topic) {
		this.writeAuthFailure(w)
		return
	}

	// get the raw POST message
	//lbr := io.LimitReader(r.Body, options.maxPubSize+1)
	buffer := mpool.BytesBufferGet() // TODO pass the r.Body directly to PubStore
	buffer.Reset()
	if _, err := io.Copy(buffer, r.Body); err != nil {
		// e,g. remote client connection broken
		mpool.BytesBufferPut(buffer)

		log.Warn("%s %+v: %s", r.RemoteAddr, params, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msgBytes := buffer.Bytes()
	if options.debug {
		log.Debug("pub[%s] %s %+v %s", appid, r.RemoteAddr, params, string(msgBytes))
	}

	var t1 time.Time
	if !options.disableMetrics {
		t1 = time.Now()
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(msgBytes)))
	}

	query := r.URL.Query() // reuse the query will save 100ns

	pubMethod := store.DefaultPubStore.SyncPub
	if query.Get(UrlQueryAsync) == "1" {
		pubMethod = store.DefaultPubStore.AsyncPub
	}
	ver := params.ByName(UrlParamVersion)
	partition, offset, err := pubMethod(meta.Default.LookupCluster(appid, topic),
		appid+"."+topic+"."+ver,
		//meta.KafkaTopic(appid, topic, params.ByName(UrlParamVersion)),
		query.Get(UrlQueryKey), msgBytes)
	if err != nil {
		mpool.BytesBufferPut(buffer) // defer is costly

		if options.enableBreaker && isBrokerError(err) {
			this.breaker.Fail()
		}

		if !options.disableMetrics {
			this.pubMetrics.pubFail(appid, topic, ver)
		}

		log.Error("%s: %v", r.RemoteAddr, err)

		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	//w.Header().Set(ContentTypeHeader, ContentTypeJson)

	// manually create the json for performance
	// use encoding/json will cost 800ns
	buffer.Reset()
	buffer.WriteString(`{"partition":`)
	buffer.WriteString(strconv.Itoa(int(partition)))
	buffer.WriteString(`,"offset":`)
	buffer.WriteString(strconv.Itoa(int(offset)))
	buffer.WriteString(`}`)
	if _, err = w.Write(buffer.Bytes()); err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		this.pubMetrics.ClientError.Inc(1)
	}
	mpool.BytesBufferPut(buffer) // defer is costly

	// TODO so many metrics, are to be put into anther thread via chan
	// DONT block the main handler thread
	if !options.disableMetrics {
		this.pubMetrics.pubOk(appid, topic, params.ByName(UrlParamVersion))
		this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
	}

}
