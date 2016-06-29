// +build !fasthttp

package gateway

import (
	"hash/adler32"
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

// POST /v1/msgs/:topic/:ver?key=mykey&async=1&ack=all&batch=1
func (this *pubServer) pubHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		appid        string
		topic        string
		ver          string
		tag          string
		partitionKey string
		t1           = time.Now()
	)

	if Options.EnableClientStats { // TODO enable pub or sub client stats
		this.gw.clientStates.RegisterPubClient(r)
	}

	realIp := getHttpRemoteIp(r)
	if Options.Ratelimit && !this.throttlePub.Pour(realIp, 1) {
		log.Warn("pub[%s] %s(%s) rate limit reached: %d/s", appid, r.RemoteAddr, realIp, Options.PubQpsLimit)

		this.pubMetrics.ClientError.Inc(1)
		writeQuotaExceeded(w)
		return
	}

	appid = r.Header.Get(HttpHeaderAppid)
	topic = params.ByName(UrlParamTopic)
	ver = params.ByName(UrlParamVersion)
	if err := manager.Default.OwnTopic(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Warn("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} %s",
			appid, r.RemoteAddr, realIp, topic, ver, r.Header.Get("User-Agent"), err)

		this.pubMetrics.ClientError.Inc(1)
		writeAuthFailure(w, err)
		return
	}

	msgLen := int(r.ContentLength)
	switch {
	case int64(msgLen) > Options.MaxPubSize:
		log.Warn("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} too big content length: %d",
			appid, r.RemoteAddr, realIp, topic, ver, r.Header.Get("User-Agent"), msgLen)

		this.pubMetrics.ClientError.Inc(1)
		writeBadRequest(w, ErrTooBigMessage.Error())
		return

	case msgLen < Options.MinPubSize:
		log.Warn("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} too small content length: %d",
			appid, r.RemoteAddr, realIp, topic, ver, r.Header.Get("User-Agent"), msgLen)

		this.pubMetrics.ClientError.Inc(1)
		writeBadRequest(w, ErrTooSmallMessage.Error())
		return
	}

	var msg *mpool.Message
	tag = r.Header.Get(HttpHeaderMsgTag)
	if tag != "" {
		if len(tag) > Options.MaxMsgTagLen {
			writeBadRequest(w, "too big tag")
			return
		}

		msg = mpool.NewMessage(tagLen(tag) + msgLen)
		msg.Body = msg.Body[0 : tagLen(tag)+msgLen]
	} else {
		msg = mpool.NewMessage(msgLen)
		msg.Body = msg.Body[0:msgLen]
	}

	// get the raw POST message
	lbr := io.LimitReader(r.Body, Options.MaxPubSize+1)
	if _, err := io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		log.Error("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} %s",
			appid, r.RemoteAddr, realIp, topic, ver, r.Header.Get("User-Agent"), err)

		this.pubMetrics.ClientError.Inc(1)
		writeBadRequest(w, ErrTooBigMessage.Error())
		return
	}

	if tag != "" {
		AddTagToMessage(msg, tag)
	}

	if Options.AuditPub {
		this.auditor.Trace("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} k:%s vlen:%d h:%d",
			appid, r.RemoteAddr, realIp, topic, ver, r.Header.Get("User-Agent"),
			partitionKey, msgLen, adler32.Checksum(msg.Body))
	}

	if !Options.DisableMetrics {
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(msg.Body)))
	}

	query := r.URL.Query() // reuse the query will save 100ns
	if query.Get("batch") == "1" {
		// TODO
	}
	partitionKey = query.Get("key")
	if len(partitionKey) > MaxPartitionKeyLen {
		log.Warn("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} too big key: %s",
			appid, r.RemoteAddr, realIp, topic, ver,
			r.Header.Get("User-Agent"), partitionKey)

		this.pubMetrics.ClientError.Inc(1)
		writeBadRequest(w, "too big key")
		return
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		log.Warn("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} cluster not found",
			appid, r.RemoteAddr, realIp, topic, r.Header.Get("User-Agent"), ver)

		this.pubMetrics.ClientError.Inc(1)
		writeBadRequest(w, "invalid appid")
		return
	}

	pubMethod := store.DefaultPubStore.SyncPub
	if query.Get("async") == "1" {
		pubMethod = store.DefaultPubStore.AsyncPub
	}
	if query.Get("ack") == "all" {
		pubMethod = store.DefaultPubStore.SyncAllPub
	}

	partition, offset, err := pubMethod(cluster,
		manager.Default.KafkaTopic(appid, topic, ver),
		[]byte(partitionKey), msg.Body)
	if err != nil {
		log.Error("pub[%s] %s(%s) {topic:%s ver:%s} %s",
			appid, r.RemoteAddr, realIp, topic, ver, err)

		msg.Free() // defer is costly

		if !Options.DisableMetrics {
			this.pubMetrics.PubFail(appid, topic, ver)
		}

		writeServerError(w, err.Error())
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

	if !Options.DisableMetrics {
		this.pubMetrics.PubOk(appid, topic, ver)
		this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
	}

}
