package gateway

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

//go:generate goannotation $GOFILE
// @rest GET /v1/raw/msgs/:topic/:ver
// tells client how to pub in raw mode: how to connect directly to kafka
func (this *manServer) pubRawHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	ver := params.ByName(UrlParamVersion)
	topic := params.ByName(UrlParamTopic)
	appid := r.Header.Get(HttpHeaderAppid)
	realIp := getHttpRemoteIp(r)

	if err := manager.Default.OwnTopic(appid, r.Header.Get(HttpHeaderPubkey), topic); err != nil {
		log.Error("raw[%s] %s(%s) {topic:%s, ver:%s}: %s",
			appid, r.RemoteAddr, realIp, topic, ver, err)

		writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(appid)
	if !found {
		writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("pub raw[%s] %s(%s): {topic:%s ver:%s}", appid, r.RemoteAddr, realIp, topic, ver)

	var out = map[string]string{
		"store":   store.DefaultPubStore.Name(),
		"topic":   manager.Default.KafkaTopic(appid, topic, ver),
		"brokers": strings.Join(meta.Default.ZkCluster(cluster).NamedBrokerList(), ","),
	}
	b, _ := json.Marshal(out)
	w.Write(b)
}
