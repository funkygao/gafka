// +build !fasthttp

package gateway

import (
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/hh"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/mpool"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

//go:generate goannotation $GOFILE
// @rest POST /v1/msgs/:topic/:ver?key=mykey&async=1&ack=all&hh=n
func (this *pubServer) pubHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	var (
		appid        string
		topic        string
		ver          string
		tag          string
		partitionKey string
		async        bool
		hhDisabled   bool // hh enabled by default
		t1           = time.Now()
	)

	if !Options.DisableMetrics {
		this.pubMetrics.PubTryQps.Mark(1)
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
		this.respond4XX(appid, w, err.Error(), http.StatusUnauthorized)
		return
	}

	msgLen := int(r.ContentLength)
	switch {
	case int64(msgLen) > Options.MaxPubSize:
		log.Warn("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} too big content length: %d",
			appid, r.RemoteAddr, realIp, topic, ver, r.Header.Get("User-Agent"), msgLen)

		this.pubMetrics.ClientError.Inc(1)
		this.respond4XX(appid, w, ErrTooBigMessage.Error(), http.StatusBadRequest)
		return

	case msgLen < Options.MinPubSize:
		log.Warn("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} too small content length: %d",
			appid, r.RemoteAddr, realIp, topic, ver, r.Header.Get("User-Agent"), msgLen)

		this.pubMetrics.ClientError.Inc(1)
		this.respond4XX(appid, w, ErrTooSmallMessage.Error(), http.StatusBadRequest)
		return
	}

	query := r.URL.Query() // reuse the query will save 100ns

	partitionKey = query.Get("key")
	if len(partitionKey) > MaxPartitionKeyLen {
		log.Warn("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} too big key: %s",
			appid, r.RemoteAddr, realIp, topic, ver,
			r.Header.Get("User-Agent"), partitionKey)

		this.pubMetrics.ClientError.Inc(1)
		this.respond4XX(appid, w, "too big key", http.StatusBadRequest)
		return
	}

	var msg *mpool.Message
	tag = r.Header.Get(HttpHeaderMsgTag)
	if tag != "" {
		if len(tag) > Options.MaxMsgTagLen {
			this.respond4XX(appid, w, "too big tag", http.StatusBadRequest)
			return
		}

		msgSz := tagLen(tag) + msgLen
		msg = mpool.NewMessage(msgSz)
		msg.Body = msg.Body[0:msgSz]
	} else {
		msg = mpool.NewMessage(msgLen)
		msg.Body = msg.Body[0:msgLen]
	}

	// get the raw POST message, if body more than content-length ignore the extra payload
	lbr := io.LimitReader(r.Body, Options.MaxPubSize+1)
	if _, err := io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		log.Error("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} %s",
			appid, r.RemoteAddr, realIp, topic, ver, r.Header.Get("User-Agent"), err)

		this.pubMetrics.ClientError.Inc(1)
		this.respond4XX(appid, w, err.Error(), http.StatusBadRequest) // TODO http.StatusRequestEntityTooLarge
		return
	}

	if tag != "" {
		AddTagToMessage(msg, tag)
	}

	if !Options.DisableMetrics {
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(msg.Body)))
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		log.Warn("pub[%s] %s(%s) {topic:%s ver:%s UA:%s} cluster not found",
			appid, r.RemoteAddr, realIp, topic, r.Header.Get("User-Agent"), ver)

		this.pubMetrics.ClientError.Inc(1)
		this.respond4XX(appid, w, "invalid appid", http.StatusBadRequest)
		return
	}

	var (
		partition int32
		offset    int64 = -1
		err       error
		rawTopic  = manager.Default.KafkaTopic(appid, topic, ver)
	)

	pubMethod := store.DefaultPubStore.SyncPub
	async = query.Get("async") == "1"
	if async {
		pubMethod = store.DefaultPubStore.AsyncPub
	}

	ackAll := query.Get("ack") == "all"
	if ackAll {
		pubMethod = store.DefaultPubStore.SyncAllPub
	}

	hhDisabled = query.Get("hh") == "n" // yes | no

	msgKey := []byte(partitionKey)
	if ackAll {
		// hh not applied
		partition, offset, err = pubMethod(cluster, rawTopic, msgKey, msg.Body)
	} else if Options.AllwaysHintedHandoff {
		err = hh.Default.Append(cluster, rawTopic, msgKey, msg.Body)
	} else if !hhDisabled && Options.EnableHintedHandoff && !hh.Default.Empty(cluster, rawTopic) {
		err = hh.Default.Append(cluster, rawTopic, msgKey, msg.Body)
	} else if async {
		if !hhDisabled && Options.EnableHintedHandoff {
			// async uses hinted handoff mechanism to save memory overhead
			err = hh.Default.Append(cluster, rawTopic, msgKey, msg.Body)
		} else {
			// message pool can't be applied on async pub because
			// we don't know when to recycle the memory
			body := make([]byte, 0, len(msg.Body))
			copy(body, msg.Body)
			partition, offset, err = pubMethod(cluster, rawTopic, msgKey, body)
		}
	} else {
		// hack byte string conv TODO
		partition, offset, err = pubMethod(cluster, rawTopic, msgKey, msg.Body)
		if err != nil {
			// sarama didn't reset this, so I have to handle it
			offset = -1
		}
		if err != nil && store.DefaultPubStore.IsSystemError(err) && !hhDisabled && Options.EnableHintedHandoff {
			log.Warn("pub[%s] %s(%s) {%s.%s.%s UA:%s} resort hh for: %v", appid, r.RemoteAddr, realIp,
				appid, topic, ver, r.Header.Get("User-Agent"), err)

			err = hh.Default.Append(cluster, rawTopic, msgKey, msg.Body)
			// async = true
		}
	}

	if err != nil {
		log.Error("pub[%s] %s(%s) {topic:%s.%s err:%s} '%s'", appid, r.RemoteAddr, realIp, topic, ver, err, string(msg.Body))
	} else if Options.AuditPub && offset > -1 {
		this.auditor.Trace("pub[%s] %s(%s) {%s.%s.%s UA:%s} {P:%d O:%d} a=%v",
			appid, r.RemoteAddr, realIp, appid, topic, ver, r.Header.Get("User-Agent"), partition, offset, async)
	}

	msg.Free()

	if err != nil {
		if !Options.DisableMetrics {
			this.pubMetrics.PubFail(appid, topic, ver)
		}

		if store.DefaultPubStore.IsSystemError(err) {
			this.pubMetrics.InternalErr.Inc(1)
			writeServerError(w, err.Error())
		} else {
			this.respond4XX(appid, w, err.Error(), http.StatusBadRequest)
		}
		return
	}

	w.Header().Set(HttpHeaderPartition, strconv.FormatInt(int64(partition), 10))
	w.Header().Set(HttpHeaderOffset, strconv.FormatInt(offset, 10))
	if async {
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusCreated)
	}

	if _, err = w.Write(ResponseOk); err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		this.pubMetrics.ClientError.Inc(1)
	}

	if !Options.DisableMetrics {
		this.pubMetrics.PubOk(appid, topic, ver)
		this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
	}

}
