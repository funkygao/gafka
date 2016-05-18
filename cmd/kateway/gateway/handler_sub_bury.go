package gateway

import (
	"io"
	"net/http"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/sla"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// PUT /v1/bury/:appid/:topic/:ver?group=xx&q=yy
func (this *subServer) buryHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic      string
		ver        string
		myAppid    string
		hisAppid   string
		group      string
		rawTopic   string
		shadow     string
		bury       string
		partition  string
		partitionN int = -1
		offset     string
		offsetN    int64 = -1
		err        error
	)

	query := r.URL.Query()
	group = query.Get("group")
	if !manager.Default.ValidateGroupName(r.Header, group) {
		writeBadRequest(w, "illegal group")
		return
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	bury = r.Header.Get(HttpHeaderMsgBury)
	if !sla.ValidateShadowName(bury) {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} illegal bury: %s",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), bury)

		writeBadRequest(w, "illegal bury")
		return
	}

	// auth
	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

		writeAuthFailure(w, err)
		return
	}

	partition = r.Header.Get(HttpHeaderPartition)
	offset = r.Header.Get(HttpHeaderOffset)
	if partition == "" || offset == "" {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} empty offset or partition",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"))

		writeBadRequest(w, "empty offset or partition")
		return
	}

	offsetN, err = strconv.ParseInt(offset, 10, 64)
	if err != nil || offsetN < 0 {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} illegal offset:%s",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), offset)

		writeBadRequest(w, "bad offset")
		return
	}
	partitionN, err = strconv.Atoi(partition)
	if err != nil || partitionN < 0 {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} illegal partition:%s",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), partition)

		writeBadRequest(w, "bad partition")
		return
	}

	shadow = query.Get("q")

	log.Debug("bury[%s] %s(%s): {app:%s bury:%s shadow=%s topic:%s ver:%s group:%s partition:%s offset:%s UA:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, bury, shadow, topic, ver,
		partition, offset, r.Header.Get("User-Agent"))

	msgLen := int(r.ContentLength)
	msg := make([]byte, 0, msgLen)
	if _, err := io.ReadAtLeast(r.Body, msg, msgLen); err != nil {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

		writeBadRequest(w, err.Error())
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} invalid appid:%s",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), hisAppid)

		writeBadRequest(w, "invalid appid")
		return
	}

	// calculate raw topic according to shadow
	if shadow != "" {
		if !sla.ValidateShadowName(shadow) {
			log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} invalid shadow name",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

			writeBadRequest(w, "invalid shadow name")
			return
		}

		if !manager.Default.IsShadowedTopic(hisAppid, topic, ver, myAppid, group) {
			log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} not a shadowed topic",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

			writeBadRequest(w, "register shadow first")
			return
		}

		rawTopic = manager.ShadowTopic(shadow, myAppid, hisAppid, topic, ver, group)
	} else {
		rawTopic = manager.KafkaTopic(hisAppid, topic, ver)
	}

	fetcher, err := store.DefaultSubStore.Fetch(cluster, rawTopic,
		myAppid+"."+group, r.RemoteAddr, "", Options.PermitStandbySub)
	if err != nil {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

		writeBadRequest(w, err.Error())
		return
	}

	// step1: pub
	shadowTopic := manager.ShadowTopic(bury, myAppid, hisAppid, topic, ver, group)
	_, _, err = store.DefaultPubStore.SyncPub(cluster, shadowTopic, nil, msg)
	if err != nil {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		writeServerError(w, err.Error())
		return
	}

	// step2: skip this message in the master topic TODO atomic with step1
	if err = fetcher.CommitUpto(&sarama.ConsumerMessage{
		Topic:     rawTopic, // FIXME it's wrong!!!
		Partition: int32(partitionN),
		Offset:    offsetN,
	}); err != nil {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		writeServerError(w, err.Error())
		return
	}

	w.Write(ResponseOk)
}
