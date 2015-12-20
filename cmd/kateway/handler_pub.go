// +build !fasthttp

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/mpool"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

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
	msgLen := int(r.ContentLength)
	if msgLen == -1 {
		this.writeInvalidContentLength(w)
		log.Warn("pub[%s] %s %+v invalid content length", appid, r.RemoteAddr, params)
		return
	}

	msg := mpool.NewMessage(msgLen)
	msg.Body = msg.Body[0:msgLen]
	if _, err := io.ReadAtLeast(r.Body, msg.Body, msgLen); err != nil {
		msg.Free()

		log.Warn("%s %+v: %s", r.RemoteAddr, params, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if options.debug {
		log.Debug("pub[%s] %s %+v %s", appid, r.RemoteAddr, params, string(msg.Body))
	}

	var t1 time.Time // FIXME should be placed at beginning of handler
	if !options.disableMetrics {
		t1 = time.Now()
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(msg.Body)))
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
		query.Get(UrlQueryKey), msg.Body)
	if err != nil {
		msg.Free() // defer is costly

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

	msg.Reset()
	msg.WriteString(`{"partition":`)
	msg.WriteString(strconv.Itoa(int(partition)))
	msg.WriteString(`,"offset":`)
	msg.WriteString(strconv.Itoa(int(offset)))
	msg.WriteString(`}`)
	if _, err = w.Write(msg.Bytes()); err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		this.pubMetrics.ClientError.Inc(1)
	}

	msg.Free()

	// TODO so many metrics, are to be put into anther thread via chan
	// DONT block the main handler thread
	if !options.disableMetrics {
		this.pubMetrics.pubOk(appid, topic, params.ByName(UrlParamVersion))
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

	if !meta.Default.AuthSub(appid, r.Header.Get(HttpHeaderPubkey), topic) {
		this.writeAuthFailure(w)
		return
	}

	cluster := meta.Default.LookupCluster(appid, topic)
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

func (this *Gateway) pubWsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("%s: %v", r.RemoteAddr, err)
		return
	}

	defer ws.Close()
}
