// +build !fasthttp

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// /topics/:topic/:ver?key=mykey&async=1&delay=100
func (this *Gateway) pubHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	t1 := time.Now()

	if options.EnableClientStats { // TODO enable pub or sub client stats
		this.clientStates.RegisterPubClient(r)
	}

	if options.Ratelimit && !this.leakyBuckets.Pour(r.RemoteAddr, 1) {
		this.writeQuotaExceeded(w)
		status = http.StatusNotAcceptable
		return
	}

	appid := r.Header.Get(HttpHeaderAppid)
	topic := params.ByName(UrlParamTopic) // params[0].Value
	ver := params.ByName(UrlParamVersion) // params[1].Value
	if err := manager.Default.AuthPub(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)

		this.writeAuthFailure(w, err)
		status = http.StatusUnauthorized
		return
	}

	// get the raw POST message
	msgLen := int(r.ContentLength)
	switch {
	case msgLen == -1:
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} invalid content length: %d",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, msgLen)

		this.writeBadRequest(w, "invalid content length")
		status = http.StatusBadRequest
		return

	case int64(msgLen) > options.MaxPubSize:
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} too big content length: %d",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, msgLen)
		this.writeBadRequest(w, ErrTooBigPubMessage.Error())
		status = http.StatusBadRequest
		return

	case msgLen < options.MinPubSize:
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} too small content length: %d",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, msgLen)
		this.writeBadRequest(w, ErrTooSmallPubMessage.Error())
		status = http.StatusBadRequest
		return
	}

	lbr := io.LimitReader(r.Body, options.MaxPubSize+1)
	msg := mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]
	if _, err := io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		log.Error("pub[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)
		this.writeBadRequest(w, ErrTooBigPubMessage.Error())
		status = http.StatusBadRequest
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

		this.writeBadRequest(w, "too large partition key")
		status = http.StatusBadRequest
		return
	}

	pubMethod := store.DefaultPubStore.SyncPub
	if query.Get("async") == "1" {
		pubMethod = store.DefaultPubStore.AsyncPub
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		log.Warn("pub[%s] %s(%s) {topic:%s, ver:%s} cluster not found",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver)

		this.writeBadRequest(w, "invalid appid")
		status = http.StatusBadRequest
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
		status = http.StatusInternalServerError
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

	return
}

// /raw/topics/:topic/:ver
func (this *Gateway) pubRawHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	var (
		topic string
		ver   string
		appid string
	)

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	appid = r.Header.Get(HttpHeaderAppid)

	if err := manager.Default.AuthSub(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Error("app[%s] %s %+v: %s", appid, r.RemoteAddr, params, err)

		this.writeAuthFailure(w, err)
		status = http.StatusUnauthorized
		return
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		log.Error("cluster not found for app: %s", appid)

		this.writeBadRequest(w, "invalid appid")
		status = http.StatusBadRequest
		return
	}

	out := map[string]string{
		"store":       "kafka",
		"broker.list": strings.Join(meta.Default.BrokerList(cluster), ","),
		"topic":       meta.KafkaTopic(appid, topic, ver),
	}
	b, _ := json.Marshal(out)
	w.Write(b)

	return
}

// /ws/topics/:topic/:ver
func (this *Gateway) pubWsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		return
	}

	defer ws.Close()
	return
}
