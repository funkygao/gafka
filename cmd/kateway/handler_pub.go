// +build !fasthttp

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// /topics/:topic/:ver?key=mykey&async=1&delay=100
func (this *Gateway) pubHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	t1 := time.Now()

	if options.Ratelimit && !this.leakyBuckets.Pour(r.RemoteAddr, 1) {
		this.writeQuotaExceeded(w)
		return
	}

	topic := params.ByName(UrlParamTopic)
	appid := r.Header.Get(HttpHeaderAppid)
	if err := meta.Default.AuthPub(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Error("app[%s] %s %+v: %s", appid, r.RemoteAddr, params, err)

		this.writeAuthFailure(w, err)
		return
	}

	// get the raw POST message
	msgLen := int(r.ContentLength)
	switch {
	case msgLen == -1:
		log.Warn("pub[%s] %s %+v invalid content length", appid, r.RemoteAddr, params)
		this.writeInvalidContentLength(w)
		return

	case int64(msgLen) > options.MaxPubSize:
		log.Warn("pub[%s] %s %+v too big content length:%d", appid, r.RemoteAddr, params, msgLen)
		this.writeErrorResponse(w, ErrTooBigPubMessage.Error(), http.StatusBadRequest)
		return

	case msgLen < options.MinPubSize:
		log.Warn("pub[%s] %s %+v too small content length:%d", appid, r.RemoteAddr, params, msgLen)
		this.writeErrorResponse(w, ErrTooSmallPubMessage.Error(), http.StatusBadRequest)
		return
	}

	lbr := io.LimitReader(r.Body, options.MaxPubSize+1)
	msg := mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]
	if _, err := io.ReadAtLeast(lbr, msg.Body, msgLen); err != nil {
		msg.Free()

		log.Error("pub[%s] %s %+v: %s", appid, r.RemoteAddr, params, err)
		this.writeErrorResponse(w, ErrTooBigPubMessage.Error(), http.StatusBadRequest)
		return
	}

	ver := params.ByName(UrlParamVersion)

	if options.Debug {
		log.Debug("pub[%s] %s %+v %s", appid, r.RemoteAddr, params, string(msg.Body))
	}

	// register the online producer
	this.produersLock.RLock()
	_, producerExists := this.producers[r.RemoteAddr]
	this.produersLock.RUnlock()
	if !producerExists {
		this.produersLock.Lock()
		this.producers[r.RemoteAddr] = Producer{
			Appid: appid,
			Topic: topic,
			Ver:   ver,
		}
		this.produersLock.Unlock()
	}

	if !options.DisableMetrics {
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(msg.Body)))
	}

	query := r.URL.Query() // reuse the query will save 100ns

	pubMethod := store.DefaultPubStore.SyncPub
	if query.Get(UrlQueryAsync) == "1" {
		pubMethod = store.DefaultPubStore.AsyncPub
	}

	cluster, found := meta.Default.LookupCluster(appid)
	if !found {
		log.Error("cluster not found for app: %s", appid)

		http.Error(w, "invalid appid", http.StatusBadRequest)
		return
	}

	err := pubMethod(cluster, appid+"."+topic+"."+ver,
		[]byte(query.Get(UrlQueryKey)), msg.Body)
	if err != nil {
		msg.Free() // defer is costly

		if !options.DisableMetrics {
			this.pubMetrics.PubFail(appid, topic, ver)
		}

		log.Error("pub[%s] %s %+v: %s", appid, r.RemoteAddr, params, err)
		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msg.Free()
	if _, err = w.Write(ResponseOk); err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		this.pubMetrics.ClientError.Inc(1)
	}

	if !options.DisableMetrics {
		this.pubMetrics.PubOk(appid, topic, ver)
		this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
	}

}

// /raw/topics/:topic/:ver
func (this *Gateway) pubRawHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	var (
		topic string
		ver   string
		appid string
	)

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	appid = r.Header.Get(HttpHeaderAppid)

	if err := meta.Default.AuthSub(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Error("app[%s] %s %+v: %s", appid, r.RemoteAddr, params, err)

		this.writeAuthFailure(w, err)
		return
	}

	cluster, found := meta.Default.LookupCluster(appid)
	if !found {
		log.Error("cluster not found for app: %s", appid)

		http.Error(w, "invalid appid", http.StatusBadRequest)
		return
	}

	this.writeKatewayHeader(w)
	var out = map[string]string{
		"store":       "kafka",
		"broker.list": strings.Join(meta.Default.BrokerList(cluster), ","),
		"topic":       meta.KafkaTopic(appid, topic, ver),
	}

	b, _ := json.Marshal(out)
	w.Header().Set(ContentTypeText, ContentTypeJson)
	w.Write(b)
}

// /ws/topics/:topic/:ver
func (this *Gateway) pubWsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		return
	}

	defer ws.Close()
}
