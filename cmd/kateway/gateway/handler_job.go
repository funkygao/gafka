package gateway

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

// POST /v1/jobs/:topic/:ver?delay=100s
func (this *pubServer) addJobHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	t1 := time.Now()

	if Options.EnableClientStats { // TODO enable pub or sub client stats
		this.gw.clientStates.RegisterPubClient(r)
	}

	appid := r.Header.Get(HttpHeaderAppid)
	realIp := getHttpRemoteIp(r)
	if Options.Ratelimit && !this.throttlePub.Pour(realIp, 1) {
		log.Warn("+job[%s] %s(%s) rate limit reached", appid, r.RemoteAddr, realIp)

		writeQuotaExceeded(w)
		return
	}

	topic := params.ByName(UrlParamTopic)
	ver := params.ByName(UrlParamVersion)
	if err := manager.Default.OwnTopic(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} %s", appid, r.RemoteAddr, realIp, topic, ver, err)

		writeAuthFailure(w, err)
		return
	}

	// get the raw POST message
	msgLen := int(r.ContentLength)
	switch {
	case msgLen == -1:
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} invalid content length: %d",
			appid, r.RemoteAddr, realIp, topic, ver, msgLen)

		writeBadRequest(w, "invalid content length")
		return

	case int64(msgLen) > Options.MaxPubSize:
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} too big content length: %d",
			appid, r.RemoteAddr, realIp, topic, ver, msgLen)
		writeBadRequest(w, ErrTooBigMessage.Error())
		return

	case msgLen < Options.MinPubSize:
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} too small content length: %d",
			appid, r.RemoteAddr, realIp, topic, ver, msgLen)
		writeBadRequest(w, ErrTooSmallMessage.Error())
		return
	}

	lbr := io.LimitReader(r.Body, Options.MaxPubSize+1)
	msg := mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]
	if _, err := io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		log.Error("+job[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, realIp, topic, ver, err)
		writeBadRequest(w, ErrTooBigMessage.Error())
		return
	}

	if Options.Debug {
		log.Debug("+job[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, realIp, topic, ver, string(msg.Body))
	}

	if !Options.DisableMetrics {
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(msg.Body)))
	}

	delay, err := time.ParseDuration(r.URL.Query().Get("delay"))
	if err != nil {
		writeBadRequest(w, "invalid delay format")
		return
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		log.Warn("+job[%s] %s(%s) {topic:%s, ver:%s} cluster not found",
			appid, r.RemoteAddr, realIp, topic, ver)

		writeBadRequest(w, "invalid appid")
		return
	}

	jobId, err := store.DefaultPubStore.AddJob(cluster,
		manager.Default.KafkaTopic(appid, topic, ver),
		msg.Body, delay)
	if err != nil {
		msg.Free() // defer is costly

		if !Options.DisableMetrics {
			this.pubMetrics.PubFail(appid, topic, ver)
		}

		log.Error("+job[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, realIp, topic, ver, err)
		writeServerError(w, err.Error())
		return
	}

	msg.Free()

	w.Header().Set(HttpHeaderJobId, jobId)
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

// DELETE /v1/jobs/:topic/:ver?id=D-1d13f5e8-9NVhoRqjowkLy6iTE/QnZw/l-05a1
func (this *pubServer) deleteJobHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	appid := r.Header.Get(HttpHeaderAppid)
	topic := params.ByName(UrlParamTopic)
	ver := params.ByName(UrlParamVersion)
	realIp := getHttpRemoteIp(r)
	if err := manager.Default.OwnTopic(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Warn("-job[%s] %s(%s) {topic:%s, ver:%s} %s",
			appid, r.RemoteAddr, realIp, topic, ver, err)

		writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		log.Warn("-job[%s] %s(%s) {topic:%s, ver:%s} cluster not found",
			appid, r.RemoteAddr, realIp, topic, ver)

		writeBadRequest(w, "invalid appid")
		return
	}

	jobId := r.URL.Query().Get("id")
	if len(jobId) < 30 { // jobId e,g. D-1d13f5e8-W6ZuLg2WVrIo6KblpXlycpze-05a1
		writeBadRequest(w, "invalid job id")
		return
	}

	if err := store.DefaultPubStore.DeleteJob(cluster, jobId); err != nil {
		log.Warn("-job[%s] %s(%s) {topic:%s, ver:%s} %v",
			appid, r.RemoteAddr, realIp, topic, ver, err)

		writeServerError(w, err.Error())
		return
	}

	w.Write(ResponseOk)
}
