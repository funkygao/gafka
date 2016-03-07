package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

func (this *Gateway) statusHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	output := make(map[string]interface{})
	output["options"] = options
	output["loglevel"] = logLevel.String()
	output["pubserver.type"] = this.pubServer.name
	b, _ := json.MarshalIndent(output, "", "    ")
	w.Write(b)

	size = len(b)
	return
}

func (this *Gateway) clientsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	b, _ := json.Marshal(this.clientStates.Export())
	w.Write(b)

	size = len(b)
	return
}

func (this *Gateway) clustersHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	b, _ := json.Marshal(meta.Default.Clusters())
	w.Write(b)

	size = len(b)
	return
}

// /options/:option/:value
func (this *Gateway) setOptionHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	option := params.ByName("option")
	value := params.ByName("value")
	boolVal := value == "true"

	switch option {
	case "debug":
		options.Debug = boolVal

	case "clients":
		options.EnableClientStats = boolVal
		this.clientStates.Reset()

	case "nometrics":
		options.DisableMetrics = boolVal

	case "ratelimit":
		options.Ratelimit = boolVal

	case "accesslog":
		options.EnableAccessLog = boolVal

	default:
		log.Warn("invalid option:%s=%s", option, value)

		this.writeBadRequest(w, "invalid option")
		return
	}

	log.Info("set option:%s to %s, %#v", option, value, options)

	w.Write(ResponseOk)

	size = len(ResponseOk)
	return
}

// /log/:level
func (this *Gateway) setlogHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	logLevel = toLogLevel(params.ByName("level"))
	for name, filter := range log.Global {
		log.Info("log[%s] level: %s -> %s", name, filter.Level, logLevel)

		filter.Level = logLevel
	}

	w.Write(ResponseOk)

	size = len(ResponseOk)
	return
}

func (this *Gateway) resetCounterHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	counterName := params.ByName("name")

	_ = counterName // TODO

	w.Write(ResponseOk)

	size = len(ResponseOk)
	return
}

func (this *Gateway) authAdmin(appid, pubkey string) (ok bool) {
	if appid == "_psubAdmin_" && pubkey == "_wandafFan_" { // FIXME
		ok = true
	}
	return
}

// /partitions/:cluster/:appid/:topic/:ver
func (this *Gateway) partitionsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	topic := params.ByName(UrlParamTopic)
	cluster := params.ByName(UrlParamCluster)
	hisAppid := params.ByName(UrlParamAppid)
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	ver := params.ByName(UrlParamVersion)
	if !this.authAdmin(appid, pubkey) {
		log.Warn("suspicous partitions call from %s(%s): {cluster:%s app:%s key:%s topic:%s ver:%s}",
			r.RemoteAddr, getHttpRemoteIp(r), cluster, appid, pubkey, topic, ver)

		this.writeAuthFailure(w, manager.ErrPermDenied)
		status = http.StatusUnauthorized
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		log.Error("cluster[%s] %v", zkcluster.Name(), err)

		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		status = http.StatusInternalServerError
		return
	}
	defer kfk.Close()

	partitions, err := kfk.Partitions(meta.KafkaTopic(hisAppid, topic, ver))
	if err != nil {
		log.Error("cluster[%s] from %s(%s) {app:%s topic:%s ver:%s} %v",
			zkcluster.Name(), r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, err)

		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		status = http.StatusInternalServerError
		return
	}

	w.Write([]byte(fmt.Sprintf(`{"num": %d}`, len(partitions))))

	size = 10
	return
}

// /topics/:cluster/:appid/:topic/:ver?partitions=1&replicas=2
func (this *Gateway) addTopicHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) (status, size int) {
	topic := params.ByName(UrlParamTopic)
	if !validateTopicName(topic) {
		log.Warn("illegal topic: %s", topic)

		this.writeBadRequest(w, "illegal topic")
		status = http.StatusBadRequest
		return
	}

	if !this.throttleAddTopic.Pour(1) {
		this.writeQuotaExceeded(w)
		status = http.StatusNotAcceptable
		return
	}

	cluster := params.ByName(UrlParamCluster)
	hisAppid := params.ByName(UrlParamAppid)
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	ver := params.ByName(UrlParamVersion)
	if !this.authAdmin(appid, pubkey) {
		log.Warn("suspicous add topic from %s(%s): {appid:%s, pubkey:%s, cluster:%s, topic:%s, ver:%s}",
			r.RemoteAddr, getHttpRemoteIp(r), appid, pubkey, cluster, topic, ver)

		this.writeAuthFailure(w, manager.ErrPermDenied)
		status = http.StatusUnauthorized
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	info := zkcluster.RegisteredInfo()
	if !info.Public {
		log.Warn("app[%s] adding topic:%s in non-public cluster: %+v", hisAppid, topic, params)

		this.writeBadRequest(w, "invalid cluster")
		status = http.StatusBadRequest
		return
	}

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

	log.Info("app[%s] from %s(%s) add topic: {appid:%s, cluster:%s, topic:%s, ver:%s query:%s}",
		appid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, cluster, topic, ver, query.Encode())

	topic = meta.KafkaTopic(hisAppid, topic, ver)
	lines, err := zkcluster.AddTopic(topic, replicas, partitions)
	if err != nil {
		log.Error("app[%s] %s add topic: %s", appid, r.RemoteAddr, err.Error())

		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		status = http.StatusInternalServerError
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
		size = len(ResponseOk)
	} else {
		this.writeErrorResponse(w, strings.Join(lines, ";"), http.StatusInternalServerError)
		status = http.StatusInternalServerError
	}

	return
}
