package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/funkygao/gafka/cmd/kateway/manager"
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
POST /topics/:topic/:ver?key=msgkey&async=<0|1>
POST /ws/topics/:topic/:ver
 GET /raw/topics/:topic/:ver
 GET /alive

sub:
 GET /topics/:appid/:topic/:ver?group=xx&limit=1&reset=<newest|oldest>&autocommit=<1|0>
 GET /ws/topics/:appid/:topic/:ver?group=xx
 GET /raw/topics/:appid/:topic/:ver
 GET /alive

man:
 GET /help
 GET /status
 GET /clusters
 GET /alive
 PUT /log/:level  level=<info|debug|trace|warn|alarm|error>
POST /topics/:cluster/:appid/:topic/:ver

dbg:
 GET /debug/pprof
 GET /debug/vars
`,
		options.PubHttpAddr, options.SubHttpAddr,
		options.ManHttpAddr, options.DebugHttpAddr))))

}

func (this *Gateway) statusHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	output := make(map[string]interface{})
	output["options"] = options
	output["loglevel"] = logLevel.String()
	output["pubserver.type"] = this.pubServer.name
	b, _ := json.MarshalIndent(output, "", "    ")
	w.Write(b)
	w.Write([]byte{'\n'})
}

func (this *Gateway) clustersHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)
	w.Header().Set(ContentTypeHeader, ContentTypeJson)
	w.WriteHeader(http.StatusOK)
	b, _ := json.Marshal(meta.Default.Clusters())
	w.Write(b)
}

func (this *Gateway) setlogHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	lvl := params.ByName("level")
	switch lvl {
	case "debug1":
		lvl = "debug"
		options.Debug = false

	case "debug2":
		lvl = "debug"
		options.Debug = true
	}

	logLevel = toLogLevel(lvl)
	for name, filter := range log.Global {
		log.Info("log[%s] level: %s -> %s", name, filter.Level, logLevel)

		filter.Level = logLevel
	}

	this.writeKatewayHeader(w)
	w.Write(ResponseOk)
}

func (this *Gateway) resetCounterHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	counterName := params.ByName("name")

	_ = counterName // TODO

	this.writeKatewayHeader(w)
	w.Write(ResponseOk)
}

// /topics/:cluster/:appid/:topic/:ver?partitions=1&replicas=2
func (this *Gateway) addTopicHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	this.writeKatewayHeader(w)

	topic := params.ByName(UrlParamTopic)
	if !validateTopicName(topic) {
		log.Warn("illegal topic: %s", topic)

		http.Error(w, "illegal topic", http.StatusBadRequest)
		return
	}

	cluster := params.ByName(UrlParamCluster)
	hisAppid := params.ByName("appid")
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	// FIXME
	if appid != "_psubAdmin_" || pubkey != "_wandafFan_" {
		log.Warn("suspicous add topic from %s: appid=%s, pubkey=%s, cluster=%s, topic=%s",
			r.RemoteAddr, appid, pubkey, cluster, topic)

		this.writeAuthFailure(w, manager.ErrPermDenied)
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	info := zkcluster.RegisteredInfo()
	if !info.Public {
		log.Warn("app[%s] adding topic:%s in non-public cluster: %+v", hisAppid, topic, params)

		http.Error(w, "invalid cluster", http.StatusBadRequest)
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

	log.Info("app[%s] %s add topic: {appid:%s, cluster:%s, topic:%s, ver:%s query:%s}",
		appid, r.RemoteAddr, hisAppid, cluster, topic, ver, query.Encode())

	topic = meta.KafkaTopic(hisAppid, topic, ver)
	lines, err := zkcluster.AddTopic(topic, replicas, partitions)
	if err != nil {
		log.Error("app[%s] %s add topic: %s", appid, r.RemoteAddr, err.Error())

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ok := false
	for _, l := range lines {
		log.Trace("app[%s] add topic[%s] in cluster %s: %s", appid, topic, cluster, l)

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
