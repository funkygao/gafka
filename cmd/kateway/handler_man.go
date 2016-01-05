package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

func (this *Gateway) helpHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Header().Set(ContentTypeHeader, ContentTypeText)
	w.Write([]byte(strings.TrimSpace(fmt.Sprintf(`
pub server: %s
sub server: %s
man server: %s
dbg server: %s

pub:
POST /topics/:topic/:ver?key=mykey&async=1
POST /ws/topics/:topic/:ver
 GET /raw/topics/:topic/:ver

sub:
 GET /topics/:appid/:topic/:ver/:group?limit=1&reset=newest
 GET /ws/topics/:appid/:topic/:ver/:group
 GET /raw/topics/:appid/:topic/:ver

man:
 GET /help
 GET /status
 GET /clusters
POST /topics/:cluster/:appid/:topic/:ver

dbg:
 GET /debug/pprof
 GET /debug/vars
`,
		options.pubHttpAddr, options.subHttpAddr,
		options.manHttpAddr, options.debugHttpAddr))))

}

func (this *Gateway) statusHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Write([]byte(fmt.Sprintf("ver:%s-%s, Built with %s-%s for %s-%s, uptime:%s",
		gafka.Version, gafka.BuildId,
		runtime.Compiler, runtime.Version(), runtime.GOOS, runtime.GOARCH,
		time.Since(this.startedAt))))
}

func (this *Gateway) clustersHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Header().Set(ContentTypeHeader, ContentTypeJson)
	w.WriteHeader(http.StatusOK)
	b, _ := json.Marshal(meta.Default.Clusters())
	w.Write(b)
}

// /topics/:cluster/:appid/:topic/:ver?partitions=1&replicas=2
func (this *Gateway) addTopicHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)

	topic := params.ByName(UrlParamTopic)
	cluster := params.ByName(UrlParamCluster)
	hisAppid := params.ByName("appid")
	appid := r.Header.Get(HttpHeaderAppid)
	if !meta.Default.AuthPub(appid, r.Header.Get(HttpHeaderPubkey), topic) {
		this.writeAuthFailure(w)
		return
	}

	ver := params.ByName(UrlParamVersion)

	replicas, partitions := 2, 1
	query := r.URL.Query()
	partitionsArg := query.Get("partitions")
	if partitionsArg != "" {
		partitions, _ = strconv.Atoi(partitionsArg)
	}
	replicasArg := query.Get("replicas")
	if replicasArg != "" {
		replicas, _ = strconv.Atoi(replicasArg)
	}

	log.Info("%s add topic: {appid:%s, cluster:%s, topic:%s, ver:%s query:%s}",
		appid, hisAppid, cluster, topic, ver, query.Encode())

	topic = meta.KafkaTopic(hisAppid, topic, ver)
	lines, err := meta.Default.ZkCluster(cluster).AddTopic(topic, replicas, partitions)
	if err != nil {
		log.Info("%s add topic: %s", appid, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ok := false
	for _, l := range lines {
		log.Info("%s add topic in cluster %s: %s", appid, cluster, l)
		if strings.Contains(l, "Created topic") {
			ok = true
		}
	}

	if ok {
		w.Write(ResponseOk)
	} else {
		http.Error(w, strings.Join(lines, "\n"), http.StatusInternalServerError)
	}
}
