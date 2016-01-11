// +build fasthttp

package main

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/buaazp/fasthttprouter"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
	"github.com/valyala/fasthttp"
)

// /topics/:topic/:ver?key=mykey&async=1&delay=100
func (this *Gateway) pubHandler(ctx *fasthttp.RequestCtx, params fasthttprouter.Params) {
	t1 := time.Now()

	topic := params.ByName(UrlParamTopic)
	header := ctx.Request.Header
	appid := string(header.Peek(HttpHeaderAppid))
	pubkey := string(header.Peek(HttpHeaderPubkey))
	if !meta.Default.AuthPub(appid, pubkey, topic) {
		log.Error("app[%s] %s %+v: auth fail", appid, ctx.RemoteAddr(), params)

		ctx.SetConnectionClose()
		ctx.Error("invalid secret", fasthttp.StatusUnauthorized)
		return
	}

	msgLen := ctx.Request.Header.ContentLength()
	switch {
	case msgLen == -1:
		log.Warn("pub[%s] %s %+v invalid content length", appid, ctx.RemoteAddr(), params)
		ctx.Error("invalid content length", fasthttp.StatusBadRequest)
		return

	case int64(msgLen) > options.maxPubSize:
		log.Warn("pub[%s] %s %+v too big content length:%d", appid, ctx.RemoteAddr(), params, msgLen)
		ctx.Error(ErrTooBigPubMessage.Error(), fasthttp.StatusBadRequest)
		return

	case msgLen < options.minPubSize:
		log.Warn("pub[%s] %s %+v too small content length:%d", appid, ctx.RemoteAddr(), params, msgLen)
		ctx.Error(ErrTooSmallPubMessage.Error(), fasthttp.StatusBadRequest)
		return
	}

	ver := params.ByName(UrlParamVersion)
	queryArgs := ctx.Request.URI().QueryArgs()
	key := queryArgs.Peek(UrlQueryKey)
	asyncArg := queryArgs.Peek(UrlQueryAsync)
	async := len(asyncArg) == 1 && asyncArg[0] == '1'
	//delay := hack.String(queryArgs.Peek(UrlQueryDelay))

	if options.debug {
		log.Debug("pub[%s] %s {topic:%s, ver:%s, key:%s, async:%+v} %s",
			appid, ctx.RemoteAddr(),
			topic, ver, key, async,
			string(ctx.Request.Body()))
	}

	if !options.disableMetrics {
		this.pubMetrics.PubQps.Mark(1)
		this.pubMetrics.PubMsgSize.Update(int64(len(ctx.PostBody())))
	}

	pubMethod := store.DefaultPubStore.SyncPub
	if async {
		pubMethod = store.DefaultPubStore.AsyncPub
	}
	err := pubMethod(meta.Default.LookupCluster(appid, topic),
		appid+"."+topic+"."+ver,
		key, ctx.PostBody())
	if err != nil {
		if !options.disableMetrics {
			this.pubMetrics.pubFail(appid, topic, ver)
		}

		log.Error("%s: %v", ctx.RemoteAddr(), err)

		ctx.Error(err.Error(), fasthttp.StatusInternalServerError)
		return
	}

	// write the reponse
	ctx.Write(ResponseOk)
	if !options.disableMetrics {
		this.pubMetrics.pubOk(appid, topic, ver)
		this.pubMetrics.PubLatency.Update(time.Since(t1).Nanoseconds() / 1e6) // in ms
	}
}

// /raw/topics/:topic/:ver
func (this *Gateway) pubRawHandler(ctx *fasthttp.RequestCtx, params fasthttprouter.Params) {
	var (
		topic  string
		ver    string
		appid  string
		pubkey string
	)

	ver = params.ByName(UrlParamVersion)
	topic = params.ByName(UrlParamTopic)
	header := ctx.Request.Header
	appid = string(header.Peek(HttpHeaderAppid))
	pubkey = string(header.Peek(HttpHeaderPubkey))

	if !meta.Default.AuthSub(appid, pubkey, topic) {
		ctx.SetConnectionClose()
		ctx.Error("invalid secret", fasthttp.StatusUnauthorized)
		return
	}

	cluster := meta.Default.LookupCluster(appid, topic)
	var out = map[string]string{
		"store":       "kafka",
		"broker.list": strings.Join(meta.Default.BrokerList(cluster), ","),
		"topic":       meta.KafkaTopic(appid, topic, ver),
	}

	b, _ := json.Marshal(out)
	ctx.SetContentType(ContentTypeJson)
	ctx.Write(b)
}

// /ws/topics/:topic/:ver
func (this *Gateway) pubWsHandler(ctx *fasthttp.RequestCtx, params fasthttprouter.Params) {
	ctx.Error("not implemented", fasthttp.StatusBadRequest)
}
