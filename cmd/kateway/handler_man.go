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

func (this *Gateway) statusHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	output := make(map[string]interface{})
	output["options"] = options
	output["loglevel"] = logLevel.String()
	output["pubserver.type"] = this.pubServer.name
	b, _ := json.MarshalIndent(output, "", "    ")
	w.Write(b)
}

func (this *Gateway) clientsHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	b, _ := json.Marshal(this.clientStates.Export())
	w.Write(b)
}

func (this *Gateway) clustersHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	b, _ := json.Marshal(meta.Default.Clusters())
	w.Write(b)
}

// /options/:option/:value
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

	case "ratelimit":
		options.Ratelimit = boolVal

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

// /log/:level
func (this *Gateway) setlogHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	logLevel = toLogLevel(params.ByName("level"))
	for name, filter := range log.Global {
		log.Info("log[%s] level: %s -> %s", name, filter.Level, logLevel)

		filter.Level = logLevel
	}

	w.Write(ResponseOk)
}

func (this *Gateway) resetCounterHandler(w http.ResponseWriter, r *http.Request,
	params httprouter.Params) {
	counterName := params.ByName("name")

	_ = counterName // TODO

	w.Write(ResponseOk)
}

// /partitions/:cluster/:appid/:topic/:ver
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

		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer kfk.Close()

	partitions, err := kfk.Partitions(meta.KafkaTopic(hisAppid, topic, ver))
	if err != nil {
		log.Error("cluster[%s] from %s(%s) {app:%s topic:%s ver:%s} %v",
			zkcluster.Name(), r.RemoteAddr, getHttpRemoteIp(r), hisAppid, topic, ver, err)

		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(fmt.Sprintf(`{"num": %d}`, len(partitions))))
}

// /topics/:cluster/:appid/:topic/:ver?partitions=1&replicas=2&retention.hours=72&retention.bytes=-1
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
		log.Warn("suspicous add topic from %s(%s): {appid:%s, pubkey:%s, cluster:%s, topic:%s, ver:%s}",
			r.RemoteAddr, getHttpRemoteIp(r), appid, pubkey, cluster, topic, ver)

		this.writeAuthFailure(w, manager.ErrPermDenied)
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	if zkcluster == nil {
		log.Error("add topic from %s(%s): {appid:%s, pubkey:%s, cluster:%s, topic:%s, ver:%s} undefined cluster",
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
	partitionsArg := query.Get(sla.SlaKeyPartitions)
	if partitionsArg != "" {
		ts.Partitions, _ = strconv.Atoi(partitionsArg)
	}
	replicasArg := query.Get(sla.SlaKeyReplicas)
	if replicasArg != "" {
		ts.Replicas, _ = strconv.Atoi(replicasArg)
	}
	retentionBytes := query.Get(sla.SlaKeyRetentionBytes)
	if retentionBytes != "" {
		ts.RetentionBytes, _ = strconv.Atoi(retentionBytes)
	}
	ts.ParseRetentionHours(query.Get(sla.SlaKeyRetentionHours))

	log.Info("app[%s] from %s(%s) add topic: {appid:%s, cluster:%s, topic:%s, ver:%s query:%s}",
		appid, r.RemoteAddr, getHttpRemoteIp(r), hisAppid, cluster, topic, ver, query.Encode())

	topic = meta.KafkaTopic(hisAppid, topic, ver)
	lines, err := zkcluster.AddTopic(topic, ts)
	if err != nil {
		log.Error("app[%s] %s add topic: %s", appid, r.RemoteAddr, err.Error())

		this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
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

		lines, err = zkcluster.ConfigTopic(topic, ts)
		if err != nil {
			log.Error("app[%s] %s alter topic: %s", appid, r.RemoteAddr, err.Error())

			this.writeErrorResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}

		for _, l := range lines {
			log.Trace("app[%s] alter topic[%s] in cluster %s: %s", appid, topic, cluster, l)
		}

		w.Write(ResponseOk)
	} else {
		this.writeErrorResponse(w, strings.Join(lines, ";"), http.StatusInternalServerError)
	}
}
