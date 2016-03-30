package main

import (
	"io"
	"net/http"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// /jobs/:topic/:ver?delay=100s
func (this *Gateway) addJobHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	t1 := time.Now()

	if options.EnableClientStats { // TODO enable pub or sub client stats
		this.clientStates.RegisterPubClient(r)
	}

	if options.Ratelimit && !this.throttlePub.Pour(getHttpRemoteIp(r), 1) {
		log.Warn("%s(%s) rate limit reached", r.RemoteAddr, getHttpRemoteIp(r))

		this.writeQuotaExceeded(w)
		return
	}

	appid := r.Header.Get(HttpHeaderAppid)
	topic := params.ByName(UrlParamTopic)
	ver := params.ByName(UrlParamVersion)
	if err := manager.Default.OwnTopic(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)

		this.writeAuthFailure(w, err)
		return
	}

	// get the raw POST message
	msgLen := int(r.ContentLength)
	switch {
	case msgLen == -1:
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} invalid content length: %d",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, msgLen)

		this.writeBadRequest(w, "invalid content length")
		return

	case int64(msgLen) > options.MaxPubSize:
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} too big content length: %d",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, msgLen)
		this.writeBadRequest(w, ErrTooBigPubMessage.Error())
		return

	case msgLen < options.MinPubSize:
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} too small content length: %d",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, msgLen)
		this.writeBadRequest(w, ErrTooSmallPubMessage.Error())
		return
	}

	lbr := io.LimitReader(r.Body, options.MaxPubSize+1)
	msg := mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]
	if _, err := io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		log.Error("+job[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)
		this.writeBadRequest(w, ErrTooBigPubMessage.Error())
		return
	}

	if options.Debug {
		log.Debug("+job[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, string(msg.Body))
	}

	if !options.DisableMetrics {
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(msg.Body)))
	}

	delay, err := time.ParseDuration(r.URL.Query().Get("delay"))
	if err != nil {
		this.writeBadRequest(w, "invalid delay format")
		return
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} cluster not found",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	jobId, err := store.DefaultPubStore.AddJob(cluster, appid+"."+topic+"."+ver,
		msg.Body, delay)
	if err != nil {
		msg.Free() // defer is costly

		if !options.DisableMetrics {
			this.pubMetrics.PubFail(appid, topic, ver)
		}

		log.Error("+job[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)
		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msg.Free()

	w.Header().Set(HttpHeaderOffset, jobId)
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

// /jobs/:topic/:ver/:id
func (this *Gateway) deleteJobHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	appid := r.Header.Get(HttpHeaderAppid)
	topic := params.ByName(UrlParamTopic)
	ver := params.ByName(UrlParamVersion)
	if err := manager.Default.OwnTopic(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Warn("-job[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)

		this.writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		log.Warn("-job[%s] %s(%s) {topic:%s, ver:%s} cluster not found",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	jobId := params.ByName("id")
	if len(jobId) < 10 { // FIXME
		this.writeBadRequest(w, "invalid job id")
		return
	}

	if err := store.DefaultPubStore.DeleteJob(cluster, jobId); err == nil {
		log.Warn("-job[%s] %s(%s) {topic:%s, ver:%s} %v",
			appid, r.RemoteAddr, getHttpRemoteIp(r), topic, ver, err)

		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
