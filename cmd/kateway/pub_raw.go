package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/julienschmidt/httprouter"
)

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
