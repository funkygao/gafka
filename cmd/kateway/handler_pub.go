// +build !fasthttp

package main

import (
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// /msgs/:topic/:ver?key=mykey&async=1&ack=all
// /topics/:topic/:ver?key=mykey&async=1&ack=all
func (this *Gateway) pubHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	t1 := time.Now()

	if options.EnableClientStats { // TODO enable pub or sub client stats
		this.clientStates.RegisterPubClient(r)
	}

	if options.Ratelimit && !this.throttlePub.Pour(getHttpRemoteIp(r), 1) {
		log.Warn("%s(%s) rate limit reached", r.RemoteAddr, getHttpRemoteIp(r))

		this.pubMetrics.ClientError.Inc(1)
		this.writeQuotaExceeded(w)
		return
	}

	appid := r.Header.Get(HttpHeaderAppid)
	topic := params.ByName(UrlParamTopic)
	ver := params.ByName(UrlParamVersion)
	if err := manager.Default.OwnTopic(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)

		this.pubMetrics.ClientError.Inc(1)
		this.writeAuthFailure(w, err)
		return
	}

	// get the raw POST message
	msgLen := int(r.ContentLength)
	switch {
	case msgLen == -1:
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} invalid content length: %d",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, msgLen)

		this.pubMetrics.ClientError.Inc(1)
		this.writeBadRequest(w, "invalid content length")
		return

	case int64(msgLen) > options.MaxPubSize:
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} too big content length: %d",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, msgLen)

		this.pubMetrics.ClientError.Inc(1)
		this.writeBadRequest(w, ErrTooBigPubMessage.Error())
		return

	case msgLen < options.MinPubSize:
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} too small content length: %d",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, msgLen)

		this.pubMetrics.ClientError.Inc(1)
		this.writeBadRequest(w, ErrTooSmallPubMessage.Error())
		return
	}

	lbr := io.LimitReader(r.Body, options.MaxPubSize+1)
	msg := mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]
	if _, err := io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		log.Error("pub[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)

		this.pubMetrics.ClientError.Inc(1)
		this.writeBadRequest(w, ErrTooBigPubMessage.Error())
		return
	}

	if options.Debug {
		log.Debug("pub[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, string(msg.Body))
	}

	if !options.DisableMetrics {
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(msg.Body)))
	}

	query := r.URL.Query() // reuse the query will save 100ns
	partitionKey := query.Get("key")
	if len(partitionKey) > MaxPartitionKeyLen {
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} too large partition key: %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, partitionKey)

		this.pubMetrics.ClientError.Inc(1)
		this.writeBadRequest(w, "too large partition key")
		return
	}

	pubMethod := store.DefaultPubStore.SyncPub
	if query.Get("async") == "1" {
		pubMethod = store.DefaultPubStore.AsyncPub
	}
	if query.Get("ack") == "all" {
		pubMethod = store.DefaultPubStore.SyncAllPub
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} cluster not found",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver)

		this.pubMetrics.ClientError.Inc(1)
		this.writeBadRequest(w, "invalid appid")
		return
	}

	partition, offset, err := pubMethod(cluster, appid+"."+topic+"."+ver,
		[]byte(partitionKey), msg.Body)
	if err != nil {
		msg.Free() // defer is costly

		if !options.DisableMetrics {
			this.pubMetrics.PubFail(appid, topic, ver)
		}

		log.Error("pub[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)
		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msg.Free()
	w.Header().Set(HttpHeaderPartition, strconv.FormatInt(int64(partition), 10))
	w.Header().Set(HttpHeaderOffset, strconv.FormatInt(offset, 10))
	w.WriteHeader(http.StatusCreated)

	if _, err = w.Write(ResponseOk); err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		this.pubMetrics.ClientError.Inc(1)
	}

	if !options.DisableMetrics {
		this.pubMetrics.PubOk(appid, topic, ver)
		this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
	}

}
