package gateway

import (
	"io"
	"net/http"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/sla"
	"github.com/funkygao/golib/hack"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// GET /v1/msgs/:appid/:topic/:ver?group=xx&&reset=<newest|oldest>&ack=1&q=<dead|retry>
func (this *Gateway) subHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic      string
		ver        string
		myAppid    string
		hisAppid   string
		reset      string
		group      string
		shadow     string
		rawTopic   string
		partition  string
		partitionN int = -1
		offset     string
		offsetN    int64    = -1
		delayedAck bool     // explicit application level acknowledgement
		tagFilters []MsgTag = nil
		err        error
	)

	if Options.EnableClientStats {
		this.clientStates.RegisterSubClient(r)
	}

	query := r.URL.Query()
	group = query.Get("group")
	reset = query.Get("reset")
	if !manager.Default.ValidateGroupName(r.Header, group) {
		this.writeBadRequest(w, "illegal group")
		return
	}

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	hisAppid = params.ByName(UrlParamAppid)
	myAppid = r.Header.Get(HttpHeaderAppid)

	// auth
	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

		this.writeAuthFailure(w, err)
		return
	}

	// fetch the client ack partition and offset
	delayedAck = query.Get("ack") == "1"
	if delayedAck {
		// consumers use explicit acknowledges in order to signal a message as processed successfully
		// if consumers fail to ACK, the message hangs and server will refuse to move ahead

		// get the partitionN and offsetN from client header
		// client will ack with partition=-1, offset=-1:
		// 1. handshake phase
		// 2. when 204 No Content
		partition = r.Header.Get(HttpHeaderPartition)
		offset = r.Header.Get(HttpHeaderOffset)
		if partition != "" && offset != "" {
			// convert partition and offset to int
			offsetN, err = strconv.ParseInt(offset, 10, 64)
			if err != nil {
				log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} offset:%s",
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
					group, r.Header.Get("User-Agent"), offset)

				this.writeBadRequest(w, "ack with bad offset")
				return
			}
			partitionN, err = strconv.Atoi(partition)
			if err != nil {
				log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} partition:%s",
					myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
					group, r.Header.Get("User-Agent"), partition)

				this.writeBadRequest(w, "ack with bad partition")
				return
			}
		} else if len(partition+offset) != 0 {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s partition:%s offset:%s UA:%s} partial ack",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, partition, offset, r.Header.Get("User-Agent"))

			this.writeBadRequest(w, "partial ack not allowed")
			return
		}
	}

	shadow = query.Get("q")

	log.Debug("sub[%s] %s(%s): {app:%s q:%s topic:%s ver:%s group:%s ack:%s partition:%s offset:%s UA:%s}",
		myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, shadow, topic, ver,
		group, query.Get("ack"), partition, offset, r.Header.Get("User-Agent"))

	// calculate raw topic according to shadow
	if shadow != "" {
		if !sla.ValidateShadowName(shadow) {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} invalid shadow name",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

			this.writeBadRequest(w, "invalid shadow name")
			return
		}

		if !manager.Default.IsShadowedTopic(hisAppid, topic, ver, myAppid, group) {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} not a shadowed topic",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

			this.writeBadRequest(w, "register shadow first")
			return
		}

		rawTopic = manager.ShadowTopic(shadow, myAppid, hisAppid, topic, ver, group)
	} else {
		rawTopic = manager.KafkaTopic(hisAppid, topic, ver)
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} cluster not found",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"))

		this.writeBadRequest(w, "invalid appid")
		return
	}

	fetcher, err := store.DefaultSubStore.Fetch(cluster, rawTopic,
		myAppid+"."+group, r.RemoteAddr, reset, Options.PermitStandbySub)
	if err != nil {
		log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

		this.writeBadRequest(w, err.Error())
		return
	}

	// commit the acked offset
	if delayedAck && partitionN >= 0 && offsetN >= 0 {
		// what if shutdown kateway now?
		// the commit will be ok, and when pumpMessages, the conn will get http.StatusNoContent
		if err = fetcher.CommitUpto(&sarama.ConsumerMessage{
			Topic:     rawTopic,
			Partition: int32(partitionN),
			Offset:    offsetN,
		}); err != nil {
			log.Error("sub commit[%s] %s(%s): {app:%s topic:%s ver:%s group:%s ack:1 partition:%s offset:%s UA:%s} %v",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, partition, offset, r.Header.Get("User-Agent"), err)

			this.writeBadRequest(w, err.Error())
			return
		} else {
			log.Debug("sub land %s(%s): {G:%s, T:%s, P:%s, O:%s}",
				r.RemoteAddr, getHttpRemoteIp(r),
				group, rawTopic, partition, offset)
		}
	}

	tag := r.Header.Get(HttpHeaderMsgTag)
	if tag != "" {
		tagFilters = parseMessageTag(tag)
	}

	err = this.pumpMessages(w, r, fetcher, myAppid, hisAppid, cluster,
		topic, ver, group, delayedAck, tagFilters)
	if err != nil {
		// e,g. broken pipe, io timeout, client gone
		log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s ack:%s partition:%s offset:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, query.Get("ack"), partition, offset, r.Header.Get("User-Agent"), err)

		this.writeServerError(w, err.Error())

		if err = fetcher.Close(); err != nil {
			log.Error("sub[%s] %s(%s): {app:%s topic:%s ver:%s group:%s} %v",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)
		}
	}
}

func (this *Gateway) pumpMessages(w http.ResponseWriter, r *http.Request,
	fetcher store.Fetcher,
	myAppid, hisAppid, cluster, topic, ver, group string,
	delayedAck bool, tagFilters []MsgTag) (err error) {
	clientGoneCh := w.(http.CloseNotifier).CloseNotify()

	select {
	case <-clientGoneCh:
		// FIXME access log will not be able to record this behavior
		err = ErrClientGone

	case <-this.shutdownCh:
		w.WriteHeader(http.StatusNoContent)
		w.Write([]byte{})

	case <-this.timer.After(Options.SubTimeout):
		w.WriteHeader(http.StatusNoContent)
		w.Write([]byte{}) // without this, client cant get response

	case msg := <-fetcher.Messages():
		partition := strconv.FormatInt(int64(msg.Partition), 10)

		w.Header().Set(HttpHeaderMsgKey, string(msg.Key))
		w.Header().Set(HttpHeaderPartition, partition)
		w.Header().Set(HttpHeaderOffset, strconv.FormatInt(msg.Offset, 10))

		var (
			tags    []MsgTag
			bodyIdx int
		)
		if IsTaggedMessage(msg.Value) {
			// TagMarkStart + tag + TagMarkEnd + body
			tags, bodyIdx, err = ExtractMessageTag(msg.Value)
			if err == nil {
				// needn't check 'index out of range' here
				w.Header().Set(HttpHeaderMsgTag, hack.String(msg.Value[1:bodyIdx-1]))
			} else {
				// not a valid tagged message, treat it as non-tagged message
			}
		}

		if len(tags) > 0 {
			// TODO compare with tagFilters
		}

		// TODO when remote close silently, the write still ok
		// which will lead to msg lost for sub
		if _, err = w.Write(msg.Value[bodyIdx:]); err != nil {
			return
		}

		if !delayedAck {
			log.Debug("sub commit offset {G:%s, T:%s, P:%d, O:%d}", group, msg.Topic, msg.Partition, msg.Offset)
			if err = fetcher.CommitUpto(msg); err != nil {
				return
			}
		} else {
			log.Debug("sub take off %s(%s): {G:%s, T:%s, P:%d, O:%d}",
				r.RemoteAddr, getHttpRemoteIp(r),
				group, msg.Topic, msg.Partition, msg.Offset)
		}

		this.subMetrics.ConsumeOk(myAppid, topic, ver)
		this.subMetrics.ConsumedOk(hisAppid, topic, ver)

	case err = <-fetcher.Errors():
		// e,g. consume a non-existent topic
		// e,g. conn with broker is broken
	}

	return
}

// PUT /v1/bury/:appid/:topic/:ver?group=xx&q=yy
func (this *Gateway) buryHandler(w http.ResponseWriter, r *http.Request,
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

	if Options.EnableClientStats {
		this.clientStates.RegisterSubClient(r)
	}

	query := r.URL.Query()
	group = query.Get("group")
	if !manager.Default.ValidateGroupName(r.Header, group) {
		this.writeBadRequest(w, "illegal group")
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

		this.writeBadRequest(w, "illegal bury")
		return
	}

	// auth
	if err = manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), err)

		this.writeAuthFailure(w, err)
		return
	}

	partition = r.Header.Get(HttpHeaderPartition)
	offset = r.Header.Get(HttpHeaderOffset)
	if partition == "" || offset == "" {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} empty offset or partition",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"))

		this.writeBadRequest(w, "empty offset or partition")
		return
	}

	offsetN, err = strconv.ParseInt(offset, 10, 64)
	if err != nil || offsetN < 0 {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} illegal offset:%s",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), offset)

		this.writeBadRequest(w, "bad offset")
		return
	}
	partitionN, err = strconv.Atoi(partition)
	if err != nil || partitionN < 0 {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} illegal partition:%s",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), partition)

		this.writeBadRequest(w, "bad partition")
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

		this.writeBadRequest(w, err.Error())
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s UA:%s} invalid appid:%s",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
			group, r.Header.Get("User-Agent"), hisAppid)

		this.writeBadRequest(w, "invalid appid")
		return
	}

	// calculate raw topic according to shadow
	if shadow != "" {
		if !sla.ValidateShadowName(shadow) {
			log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} invalid shadow name",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

			this.writeBadRequest(w, "invalid shadow name")
			return
		}

		if !manager.Default.IsShadowedTopic(hisAppid, topic, ver, myAppid, group) {
			log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s q:%s UA:%s} not a shadowed topic",
				myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver,
				group, shadow, r.Header.Get("User-Agent"))

			this.writeBadRequest(w, "register shadow first")
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

		this.writeBadRequest(w, err.Error())
		return
	}

	// step1: pub
	shadowTopic := manager.ShadowTopic(bury, myAppid, hisAppid, topic, ver, group)
	_, _, err = store.DefaultPubStore.SyncPub(cluster, shadowTopic, nil, msg)
	if err != nil {
		log.Error("bury[%s] %s(%s): {app:%s topic:%s ver:%s group:%s} %v",
			myAppid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, group, err)

		this.writeServerError(w, err.Error())
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

		this.writeServerError(w, err.Error())
		return
	}

	w.Write(ResponseOk)
}
