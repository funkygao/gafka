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
	"github.com/funkygao/gafka/sla"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

// GET /v1/status
func (this *Gateway) statusHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	output := make(map[string]interface{})
	output["options"] = options
	output["loglevel"] = logLevel.String()
	output["pubserver.type"] = this.pubServer.name
	output["manager"] = manager.Default.Dump()
	b, _ := json.MarshalIndent(output, "", "    ")
	w.Write(b)
}

// GET /v1/clients
func (this *Gateway) clientsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	b, _ := json.Marshal(this.clientStates.Export())
	w.Write(b)
}

// GET /v1/clusters
func (this *Gateway) clustersHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	b, _ := json.Marshal(meta.Default.Clusters())
	w.Write(b)
}

// PUT /v1/options/:option/:value
func (this *Gateway) setOptionHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
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

	case "gzip":
		options.EnableGzip = boolVal

	case "ratelimit":
		options.Ratelimit = boolVal

	case "maxreq":
		options.MaxRequestPerConn, _ = strconv.Atoi(value)

	case "accesslog":
		if options.EnableAccessLog != boolVal {
			// on/off switching
			if boolVal {
				this.accessLogger.Start()
			} else {
				this.accessLogger.Stop()
			}
		}
		options.EnableAccessLog = boolVal

	default:
		log.Warn("invalid option:%s=%s", option, value)

		this.writeBadRequest(w, "invalid option")
		return
	}

	log.Info("set option:%s to %s, %#v", option, value, options)

	w.Write(ResponseOk)
}

// PUT /v1/log/:level
func (this *Gateway) setlogHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	logLevel = toLogLevel(params.ByName("level"))
	for name, filter := range log.Global {
		log.Info("log[%s] level: %s -> %s", name, filter.Level, logLevel)

		filter.Level = logLevel
	}

	w.Write(ResponseOk)
}

// DELETE /v1/counter/:name
func (this *Gateway) resetCounterHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	counterName := params.ByName("name")

	_ = counterName // TODO

	w.Write(ResponseOk)
}

// GET /v1/partitions/:cluster/:appid/:topic/:ver
func (this *Gateway) partitionsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	topic := params.ByName(UrlParamTopic)
	cluster := params.ByName(UrlParamCluster)
	hisAppid := params.ByName(UrlParamAppid)
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	ver := params.ByName(UrlParamVersion)
	if !manager.Default.AuthAdmin(appid, pubkey) {
		log.Warn("suspicous partitions call from %s(%s): {cluster:%s app:%s key:%s topic:%s ver:%s}",
			r.RemoteAddr, getHttpRemoteIp(r), cluster, appid, pubkey, topic, ver)

		this.writeAuthFailure(w, manager.ErrPermDenied)
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	if zkcluster == nil {
		log.Error("suspicous partitions call from %s(%s): {cluster:%s app:%s key:%s topic:%s ver:%s} undefined cluster",
			r.RemoteAddr, getHttpRemoteIp(r), cluster, appid, pubkey, topic, ver)

		this.writeBadRequest(w, "undefined cluster")
		return
	}

	kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		log.Error("cluster[%s] %v", zkcluster.Name(), err)

		this.writeServerError(w, err.Error())
		return
	}
	defer kfk.Close()

	partitions, err := kfk.Partitions(manager.KafkaTopic(hisAppid, topic, ver))
	if err != nil {
		log.Error("cluster[%s] from %s(%s) {app:%s topic:%s ver:%s} %v",
			zkcluster.Name(), r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, err)

		this.writeServerError(w, err.Error())
		return
	}

	w.Write([]byte(fmt.Sprintf(`{"num": %d}`, len(partitions))))
}

// POST /v1/topics/:cluster/:appid/:topic/:ver?partitions=1&replicas=2&retention.hours=72&retention.bytes=-1
func (this *Gateway) addTopicHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	topic := params.ByName(UrlParamTopic)
	if !manager.Default.ValidateTopicName(topic) {
		log.Warn("illegal topic: %s", topic)

		this.writeBadRequest(w, "illegal topic")
		return
	}

	if !this.throttleAddTopic.Pour(getHttpRemoteIp(r), 1) {
		this.writeQuotaExceeded(w)
		return
	}

	cluster := params.ByName(UrlParamCluster)
	hisAppid := params.ByName(UrlParamAppid)
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	ver := params.ByName(UrlParamVersion)
	if !manager.Default.AuthAdmin(appid, pubkey) {
		log.Warn("suspicous add topic from %s(%s): {appid:%s pubkey:%s cluster:%s topic:%s ver:%s}",
			r.RemoteAddr, getHttpRemoteIp(r), appid, pubkey, cluster, topic, ver)

		this.writeAuthFailure(w, manager.ErrPermDenied)
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	if zkcluster == nil {
		log.Error("add topic from %s(%s): {appid:%s pubkey:%s cluster:%s topic:%s ver:%s} undefined cluster",
			r.RemoteAddr, getHttpRemoteIp(r), appid, pubkey, cluster, topic, ver)

		this.writeBadRequest(w, "undefined cluster")
		return
	}

	info := zkcluster.RegisteredInfo()
	if !info.Public {
		log.Warn("app[%s] adding topic:%s in non-public cluster: %+v", hisAppid, topic, params)

		this.writeBadRequest(w, "invalid cluster")
		return
	}

	ts := sla.DefaultSla()
	query := r.URL.Query()
	if partitionsArg := query.Get(sla.SlaKeyPartitions); partitionsArg != "" {
		ts.Partitions, _ = strconv.Atoi(partitionsArg)
	}
	if replicasArg := query.Get(sla.SlaKeyReplicas); replicasArg != "" {
		ts.Replicas, _ = strconv.Atoi(replicasArg)
	}
	if retentionBytes := query.Get(sla.SlaKeyRetentionBytes); retentionBytes != "" {
		ts.RetentionBytes, _ = strconv.Atoi(retentionBytes)
	}
	ts.ParseRetentionHours(query.Get(sla.SlaKeyRetentionHours))

	// validate the sla
	if err := ts.Validate(); err != nil {
		log.Error("app[%s] update topic:%s %s: %+v", hisAppid, topic, query.Encode(), err)

		this.writeBadRequest(w, err.Error())
		return
	}

	log.Info("app[%s] from %s(%s) add topic: {appid:%s cluster:%s topic:%s ver:%s query:%s}",
		appid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, cluster, topic, ver, query.Encode())

	topic = manager.KafkaTopic(hisAppid, topic, ver)
	lines, err := zkcluster.AddTopic(topic, ts)
	if err != nil {
		log.Error("app[%s] %s add topic: %s", appid, r.RemoteAddr, err.Error())

		this.writeServerError(w, err.Error())
		return
	}

	createdOk := false
	for _, l := range lines {
		log.Trace("app[%s] add topic[%s] in cluster %s: %s", appid, topic, cluster, l)

		if strings.Contains(l, "Created topic") {
			createdOk = true
		}
	}

	if createdOk {
		alterConfig := ts.DumpForAlterTopic()
		if len(alterConfig) == 0 {
			w.Write(ResponseOk)
			return
		}

		lines, err = zkcluster.AlterTopic(topic, ts)
		if err != nil {
			log.Error("app[%s] %s alter topic: %s", appid, r.RemoteAddr, err.Error())

			this.writeServerError(w, err.Error())
			return
		}

		for _, l := range lines {
			log.Trace("app[%s] alter topic[%s] in cluster %s: %s", appid, topic, cluster, l)
		}

		w.Write(ResponseOk)
	} else {
		this.writeServerError(w, strings.Join(lines, ";"))
	}
}

// PUT /v1/topics/:cluster/:appid/:topic/:ver?partitions=1&retention.hours=72&retention.bytes=-1
func (this *Gateway) updateTopicHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	topic := params.ByName(UrlParamTopic)
	if !manager.Default.ValidateTopicName(topic) {
		log.Warn("illegal topic: %s", topic)

		this.writeBadRequest(w, "illegal topic")
		return
	}

	if !this.throttleAddTopic.Pour(getHttpRemoteIp(r), 1) {
		this.writeQuotaExceeded(w)
		return
	}

	cluster := params.ByName(UrlParamCluster)
	hisAppid := params.ByName(UrlParamAppid)
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	ver := params.ByName(UrlParamVersion)
	if !manager.Default.AuthAdmin(appid, pubkey) {
		log.Warn("suspicous update topic from %s(%s): {appid:%s pubkey:%s cluster:%s topic:%s ver:%s}",
			r.RemoteAddr, getHttpRemoteIp(r), appid, pubkey, cluster, topic, ver)

		this.writeAuthFailure(w, manager.ErrPermDenied)
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	if zkcluster == nil {
		log.Error("update topic from %s(%s): {appid:%s pubkey:%s cluster:%s topic:%s ver:%s} undefined cluster",
			r.RemoteAddr, getHttpRemoteIp(r), appid, pubkey, cluster, topic, ver)

		this.writeBadRequest(w, "undefined cluster")
		return
	}

	info := zkcluster.RegisteredInfo()
	if !info.Public {
		log.Warn("app[%s] update topic:%s in non-public cluster: %+v", hisAppid, topic, params)

		this.writeBadRequest(w, "invalid cluster")
		return
	}

	ts := sla.DefaultSla()
	query := r.URL.Query()
	if partitionsArg := query.Get(sla.SlaKeyPartitions); partitionsArg != "" {
		ts.Partitions, _ = strconv.Atoi(partitionsArg)
	}
	if retentionBytes := query.Get(sla.SlaKeyRetentionBytes); retentionBytes != "" {
		ts.RetentionBytes, _ = strconv.Atoi(retentionBytes)
	}
	ts.ParseRetentionHours(query.Get(sla.SlaKeyRetentionHours))

	// validate the sla
	if err := ts.Validate(); err != nil {
		log.Error("app[%s] update topic:%s %s: %+v", hisAppid, topic, query.Encode(), err)

		this.writeBadRequest(w, err.Error())
		return
	}

	log.Info("app[%s] from %s(%s) update topic: {appid:%s cluster:%s topic:%s ver:%s query:%s}",
		appid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, cluster, topic, ver, query.Encode())

	rawTopic := manager.KafkaTopic(hisAppid, topic, ver)
	alterConfig := ts.DumpForAlterTopic()
	if len(alterConfig) == 0 {
		log.Warn("app[%s] from %s(%s) update topic: {appid:%s cluster:%s topic:%s ver:%s query:%s} nothing updated",
			appid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, cluster, topic, ver, query.Encode())

		w.Write(ResponseOk)
		return
	}

	lines, err := zkcluster.AlterTopic(rawTopic, ts)
	if err != nil {
		log.Error("app[%s] from %s(%s) update topic: {appid:%s cluster:%s topic:%s ver:%s query:%s} %v",
			appid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, cluster, topic, ver, query.Encode(), err)

		this.writeServerError(w, err.Error())
		return
	}

	for _, l := range lines {
		log.Trace("app[%s] update topic[%s] in cluster %s: %s", appid, rawTopic, cluster, l)
	}

	w.Write(ResponseOk)
}
