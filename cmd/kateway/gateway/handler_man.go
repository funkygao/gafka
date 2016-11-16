package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kateway/hh"
	"github.com/funkygao/gafka/cmd/kateway/job"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/sla"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/golib/gofmt"
	"github.com/funkygao/httprouter"
	log "github.com/funkygao/log4go"
)

//go:generate goannotation $GOFILE
// @rest GET /v1/schema/:appid/:topic/:ver
func (this *manServer) schemaHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	hisAppid := params.ByName(UrlParamAppid)
	myAppid := r.Header.Get(HttpHeaderAppid)
	topic := params.ByName(UrlParamTopic)
	ver := params.ByName(UrlParamVersion)
	realIp := getHttpRemoteIp(r)

	log.Info("schema[%s] %s(%s) {app:%s topic:%s ver:%s UA:%s}",
		myAppid, r.RemoteAddr, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"))

	// TODO authorization

	_, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		writeBadRequest(w, "invalid appid")
		return
	}

	// TODO lookup from manager and send reponse
	schema, err := manager.Default.TopicSchema(hisAppid, topic, ver)
	if err != nil {
		writeBadRequest(w, err.Error())
		return
	}

	w.Write([]byte(strings.TrimSpace(schema)))
}

// @rest GET /v1/status
func (this *manServer) statusHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	log.Info("status %s(%s)", r.RemoteAddr, getHttpRemoteIp(r))

	output := make(map[string]interface{})
	output["options"] = Options
	output["loglevel"] = logLevel.String()
	output["manager"] = manager.Default.Dump()
	pubConns := int(atomic.LoadInt32(&this.gw.pubServer.activeConnN))
	subConns := int(atomic.LoadInt32(&this.gw.subServer.activeConnN))
	output["pubconn"] = strconv.Itoa(pubConns)
	output["subconn"] = strconv.Itoa(subConns)
	output["hh_appends"] = strconv.FormatInt(hh.Default.AppendN(), 10)
	output["hh_delivers"] = strconv.FormatInt(hh.Default.DeliverN(), 10)
	output["goroutines"] = strconv.Itoa(runtime.NumGoroutine())

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	output["heap"] = gofmt.ByteSize(mem.HeapSys).String()
	output["objects"] = gofmt.Comma(int64(mem.HeapObjects))

	b, err := json.MarshalIndent(output, "", "    ")
	if err != nil {
		log.Error("%s(%s) %v", r.RemoteAddr, getHttpRemoteIp(r), err)
	}

	w.Write(b)
}

// @rest GET /v1/clusters
func (this *manServer) clustersHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	log.Info("clusters %s(%s)", r.RemoteAddr, getHttpRemoteIp(r))

	b, _ := json.Marshal(meta.Default.AssignClusters())
	w.Write(b)
}

// @rest PUT /v1/options/:option/:value
func (this *manServer) setOptionHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	option := params.ByName("option")
	value := params.ByName("value")
	boolVal := value == "true"

	// TODO auth

	switch option {
	case "debug":
		Options.Debug = boolVal

	case "nometrics":
		Options.DisableMetrics = boolVal

	case "gzip":
		Options.EnableGzip = boolVal

	case "badgroup_rater":
		Options.BadGroupRateLimit = boolVal

	case "badpub_rater":
		Options.BadPubAppRateLimit = boolVal

	case "refreshdb":
		manager.Default.ForceRefresh()

	case "ratelimit":
		Options.Ratelimit = boolVal

	case "resethh":
		hh.Default.ResetCounters()

	case "hh":
		Options.EnableHintedHandoff = boolVal
		if !boolVal {
			hh.Default.Stop()
			w.Write([]byte(fmt.Sprintf("id:%s hh[%s] stopped", Options.Id, hh.Default.Name())))
			return
		} else {
			if err := hh.Default.Start(); err != nil {
				writeServerError(w, err.Error())
				return
			} else {
				w.Write([]byte(fmt.Sprintf("id:%s hh[%s] started", Options.Id, hh.Default.Name())))
				return
			}
		}

	case "hhflush":
		if boolVal {
			if hh.Default == nil {
				writeServerError(w, "no underlying hinted handoff")
			} else if Options.EnableHintedHandoff {
				writeBadRequest(w, "turn off hinted handoff first")
			} else {
				hh.Default.FlushInflights()
				w.Write([]byte(fmt.Sprintf("id:%s hh[%s] inflights flushed", Options.Id, hh.Default.Name())))
			}
			return
		}

	case "jobshardid":
		shardId, err := strconv.Atoi(value)
		if err != nil {
			writeBadRequest(w, "invalid job shard id")
			return
		} else {
			Options.AssignJobShardId = shardId
		}

	case "punish":
		d, err := time.ParseDuration(value)
		if err != nil {
			writeBadRequest(w, err.Error())
			return
		} else {
			Options.BadClientPunishDuration = d
		}

	case "500backoff":
		d, err := time.ParseDuration(value)
		if err != nil {
			writeBadRequest(w, err.Error())
			return
		} else {
			Options.InternalServerErrorBackoff = d
		}

	case "auditpub":
		Options.AuditPub = boolVal

	case "auditsub":
		Options.AuditSub = boolVal

	case "allhh":
		Options.AllwaysHintedHandoff = boolVal

	case "standbysub":
		Options.PermitStandbySub = boolVal

	case "unregroup":
		Options.PermitUnregisteredGroup = boolVal
		manager.Default.AllowSubWithUnregisteredGroup(boolVal)

	case "loglevel":
		logLevel = toLogLevel(value)
		for _, filter := range log.Global {
			filter.Level = logLevel
		}

	case "maxreq":
		Options.MaxRequestPerConn, _ = strconv.Atoi(value)

	case "accesslog":
		if Options.EnableAccessLog != boolVal {
			// on/off switching
			if boolVal {
				this.gw.accessLogger.Start()
			} else {
				this.gw.accessLogger.Stop()
			}
		}
		Options.EnableAccessLog = boolVal

	default:
		log.Warn("invalid option:%s=%s", option, value)
		writeBadRequest(w, "invalid option")
		return
	}

	log.Info("option %s(%s) %s to %s, %#v", r.RemoteAddr, getHttpRemoteIp(r), option, value, Options)

	w.Write(ResponseOk)
}

// @rest GET /v1/partitions/:appid/:topic/:ver
func (this *manServer) partitionsHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	topic := params.ByName(UrlParamTopic)
	hisAppid := params.ByName(UrlParamAppid)
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	ver := params.ByName(UrlParamVersion)
	realIp := getHttpRemoteIp(r)

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("partitions[%s] %s(%s) {app:%s topic:%s ver:%s} invalid appid",
			appid, r.RemoteAddr, realIp, hisAppid, topic, ver)

		writeBadRequest(w, "invalid appid")
		return
	}

	if !manager.Default.AuthAdmin(appid, pubkey) {
		log.Warn("suspicous partitions call from %s(%s) {cluster:%s app:%s key:%s topic:%s ver:%s}",
			r.RemoteAddr, realIp, cluster, appid, pubkey, topic, ver)

		writeAuthFailure(w, manager.ErrAuthenticationFail)
		return
	}

	log.Info("partitions[%s] %s(%s) {cluster:%s app:%s topic:%s ver:%s}",
		appid, r.RemoteAddr, realIp, cluster, hisAppid, topic, ver)

	zkcluster := meta.Default.ZkCluster(cluster)
	if zkcluster == nil {
		log.Error("suspicous partitions call from %s(%s) {cluster:%s app:%s key:%s topic:%s ver:%s} undefined cluster",
			r.RemoteAddr, realIp, cluster, appid, pubkey, topic, ver)

		writeBadRequest(w, "undefined cluster")
		return
	}

	kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
	if err != nil {
		log.Error("cluster[%s] %v", zkcluster.Name(), err)

		writeServerError(w, err.Error())
		return
	}
	defer kfk.Close()

	partitions, err := kfk.Partitions(manager.Default.KafkaTopic(hisAppid, topic, ver))
	if err != nil {
		log.Error("cluster[%s] from %s(%s) {app:%s topic:%s ver:%s} %v",
			zkcluster.Name(), r.RemoteAddr, realIp, hisAppid, topic, ver, err)

		writeServerError(w, err.Error())
		return
	}

	w.Write([]byte(fmt.Sprintf(`{"num": %d}`, len(partitions))))
}

// @rest PUT /v1/webhook/:appid/:topic/:ver?group=xx
func (this *manServer) createWebhookHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	topic := params.ByName(UrlParamTopic)
	if !manager.Default.ValidateTopicName(topic) {
		log.Warn("illegal topic: %s", topic)

		writeBadRequest(w, "illegal topic")
		return
	}

	query := r.URL.Query()
	group := query.Get("group")
	realIp := getHttpRemoteIp(r)
	hisAppid := params.ByName(UrlParamAppid)
	myAppid := r.Header.Get(HttpHeaderAppid)
	ver := params.ByName(UrlParamVersion)

	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("+webhook[%s/%s] -(%s): {%s.%s.%s UA:%s} %v",
			myAppid, group, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"), err)

		writeAuthFailure(w, err)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("+webhook[%s/%s] -(%s): {%s.%s.%s UA:%s} undefined cluster",
			myAppid, group, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"))

		writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("+webhook[%s/%s] %s(%s): {%s.%s.%s UA:%s}",
		myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"))

	rawTopic := manager.Default.KafkaTopic(hisAppid, topic, ver)
	var hook zk.WebhookMeta
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&hook); err != nil {
		log.Error("+webhook[%s/%s] %s(%s): {%s.%s.%s UA:%s} %v",
			myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"), err)

		writeBadRequest(w, err.Error())
		return
	}
	r.Body.Close()

	// validate the url
	for _, ep := range hook.Endpoints {
		_, err := url.ParseRequestURI(ep)
		if err != nil {
			log.Error("+webhook[%s/%s] %s(%s): {%s.%s.%s UA:%s} %+v %v",
				myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"), hook.Endpoints, err)

			writeBadRequest(w, err.Error())
			return
		}
	}

	hook.Cluster = cluster // cluster is decided by server
	if err := this.gw.zkzone.CreateOrUpdateWebhook(rawTopic, hook); err != nil {
		log.Error("+webhook[%s/%s] %s(%s): {%s.%s.%s UA:%s} %v",
			myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"), err)

		writeServerError(w, err.Error())
		return
	}

	w.Write(ResponseOk)
}

// @rest DELETE /v1/jobs/:appid/:topic/:ver?group=xx
func (this *manServer) deleteWebhookHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	topic := params.ByName(UrlParamTopic)
	if !manager.Default.ValidateTopicName(topic) {
		log.Warn("illegal topic: %s", topic)

		writeBadRequest(w, "illegal topic")
		return
	}

	query := r.URL.Query()
	group := query.Get("group")
	realIp := getHttpRemoteIp(r)
	hisAppid := params.ByName(UrlParamAppid)
	myAppid := r.Header.Get(HttpHeaderAppid)
	ver := params.ByName(UrlParamVersion)

	if err := manager.Default.AuthSub(myAppid, r.Header.Get(HttpHeaderSubkey),
		hisAppid, topic, group); err != nil {
		log.Error("+webhook[%s/%s] -(%s): {%s.%s.%s UA:%s} %v",
			myAppid, group, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"), err)

		writeAuthFailure(w, err)
		return
	}

	/*
		cluster, found := manager.Default.LookupCluster(hisAppid)
		if !found {
			log.Error("+webhook[%s/%s] -(%s): {%s.%s.%s UA:%s} undefined cluster",
				myAppid, group, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"))

			writeBadRequest(w, "invalid appid")
			return
		}

		log.Info("+webhook[%s/%s] %s(%s): {%s.%s.%s UA:%s}",
			myAppid, group, r.RemoteAddr, realIp, hisAppid, topic, ver, r.Header.Get("User-Agent"))

		rawTopic := manager.Default.KafkaTopic(hisAppid, topic, ver)
	*/

}

// @rest POST /v1/jobs/:appid/:topic/:ver
func (this *manServer) createJobHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	topic := params.ByName(UrlParamTopic)
	if !manager.Default.ValidateTopicName(topic) {
		log.Warn("illegal topic: %s", topic)

		writeBadRequest(w, "illegal topic")
		return
	}

	realIp := getHttpRemoteIp(r)

	if !this.throttleAddTopic.Pour(realIp, 1) {
		writeQuotaExceeded(w)
		return
	}

	hisAppid := params.ByName(UrlParamAppid)
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	ver := params.ByName(UrlParamVersion)
	if !manager.Default.AuthAdmin(appid, pubkey) {
		log.Warn("suspicous create job %s(%s) {appid:%s pubkey:%s topic:%s ver:%s}",
			r.RemoteAddr, realIp, appid, pubkey, topic, ver)

		writeAuthFailure(w, manager.ErrAuthenticationFail)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("create job %s(%s) {appid:%s topic:%s ver:%s} invalid appid",
			r.RemoteAddr, realIp, hisAppid, topic, ver)

		writeBadRequest(w, "invalid appid")
		return
	}

	log.Info("create job[%s] %s(%s) {appid:%s topic:%s ver:%s}",
		appid, r.RemoteAddr, realIp, hisAppid, topic, ver)

	rawTopic := manager.Default.KafkaTopic(hisAppid, topic, ver)
	if err := job.Default.CreateJobQueue(Options.AssignJobShardId, hisAppid, rawTopic); err != nil {
		log.Error("create job[%s] %s(%s) {shard:%d appid:%s topic:%s ver:%s} %v",
			appid, r.RemoteAddr, realIp, Options.AssignJobShardId, hisAppid, topic, ver, err)

		writeServerError(w, err.Error())
		return
	}

	if err := this.gw.zkzone.CreateJobQueue(rawTopic, cluster); err != nil {
		log.Error("app[%s] %s(%s) create job: {shard:%d appid:%s topic:%s ver:%s} %v",
			appid, r.RemoteAddr, realIp, Options.AssignJobShardId, hisAppid, topic, ver, err)

		writeServerError(w, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write(ResponseOk)
}

// @rest POST /v1/topics/:appid/:topic/:ver?partitions=1&replicas=2&retention.hours=72&retention.bytes=-1
func (this *manServer) createTopicHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	topic := params.ByName(UrlParamTopic)
	if !manager.Default.ValidateTopicName(topic) {
		log.Warn("illegal topic: %s", topic)

		writeBadRequest(w, "illegal topic")
		return
	}

	realIp := getHttpRemoteIp(r)

	if !this.throttleAddTopic.Pour(realIp, 1) {
		writeQuotaExceeded(w)
		return
	}

	hisAppid := params.ByName(UrlParamAppid)
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	ver := params.ByName(UrlParamVersion)

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("create topic[%s] %s(%s) {appid:%s topic:%s ver:%s} invalid appid",
			appid, r.RemoteAddr, realIp, hisAppid, topic, ver)

		writeBadRequest(w, "invalid appid")
		return
	}

	if !manager.Default.AuthAdmin(appid, pubkey) {
		log.Warn("suspicous create topic %s(%s) {appid:%s pubkey:%s cluster:%s topic:%s ver:%s}",
			r.RemoteAddr, realIp, appid, pubkey, cluster, topic, ver)

		writeAuthFailure(w, manager.ErrAuthenticationFail)
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	if zkcluster == nil {
		log.Error("create topic[%s] %s(%s) {appid:%s cluster:%s topic:%s ver:%s} undefined cluster",
			appid, r.RemoteAddr, realIp, hisAppid, cluster, topic, ver)

		writeBadRequest(w, "undefined cluster")
		return
	}

	info := zkcluster.RegisteredInfo()
	if !info.Public {
		log.Warn("create topic[%s] %s(%s) adding topic:%s in non-public cluster: %+v", hisAppid, r.RemoteAddr, realIp, topic, params)

		writeBadRequest(w, "invalid cluster")
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
		log.Error("app[%s] %s(%s) update topic:%s %s: %+v", hisAppid, r.RemoteAddr, realIp, topic, query.Encode(), err)

		writeBadRequest(w, err.Error())
		return
	}

	log.Info("app[%s] %s(%s) create topic: {appid:%s cluster:%s topic:%s ver:%s query:%s}",
		appid, r.RemoteAddr, realIp, hisAppid, cluster, topic, ver, query.Encode())

	rawTopic := manager.Default.KafkaTopic(hisAppid, topic, ver)
	lines, err := zkcluster.AddTopic(rawTopic, ts)
	if err != nil {
		log.Error("app[%s] %s(%s) create topic[%s]: %s", appid, r.RemoteAddr, realIp, rawTopic, err.Error())

		writeServerError(w, err.Error())
		return
	}

	createdOk := false
	for _, l := range lines {
		log.Trace("app[%s] %s(%s) create topic[%s] in cluster %s: %s", appid, r.RemoteAddr, realIp, rawTopic, cluster, l)

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

		lines, err = zkcluster.AlterTopic(rawTopic, ts)
		if err != nil {
			log.Error("app[%s] %s(%s) alter topic[%s]: %s", appid, r.RemoteAddr, realIp, rawTopic, err.Error())

			writeServerError(w, err.Error())
			return
		}

		for _, l := range lines {
			log.Trace("app[%s] %s(%s) alter topic[%s] in cluster %s: %s", appid, r.RemoteAddr, realIp, rawTopic, cluster, l)
		}

		w.Write(ResponseOk)
	} else {
		writeServerError(w, strings.Join(lines, ";"))
	}
}

// @rest PUT /v1/topics/:appid/:topic/:ver?partitions=1&retention.hours=72&retention.bytes=-1
func (this *manServer) alterTopicHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	topic := params.ByName(UrlParamTopic)
	if !manager.Default.ValidateTopicName(topic) {
		log.Warn("illegal topic: %s", topic)

		writeBadRequest(w, "illegal topic")
		return
	}

	realIp := getHttpRemoteIp(r)

	if !this.throttleAddTopic.Pour(realIp, 1) {
		writeQuotaExceeded(w)
		return
	}

	hisAppid := params.ByName(UrlParamAppid)
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	ver := params.ByName(UrlParamVersion)
	if !manager.Default.AuthAdmin(appid, pubkey) {
		log.Warn("suspicous alter topic from %s(%s) {appid:%s pubkey:%s topic:%s ver:%s}",
			r.RemoteAddr, realIp, appid, pubkey, topic, ver)

		writeAuthFailure(w, manager.ErrAuthenticationFail)
		return
	}

	cluster, found := manager.Default.LookupCluster(hisAppid)
	if !found {
		log.Error("alter topic[%s] %s(%s) {app:%s topic:%s ver:%s} invalid appid",
			appid, r.RemoteAddr, realIp, hisAppid, topic, ver)

		writeBadRequest(w, "invalid appid")
		return
	}

	zkcluster := meta.Default.ZkCluster(cluster)
	if zkcluster == nil {
		log.Error("alter topic from %s(%s) {appid:%s pubkey:%s cluster:%s topic:%s ver:%s} undefined cluster",
			r.RemoteAddr, realIp, appid, pubkey, cluster, topic, ver)

		writeBadRequest(w, "undefined cluster")
		return
	}

	info := zkcluster.RegisteredInfo()
	if !info.Public {
		log.Warn("app[%s] alter topic:%s in non-public cluster: %+v", hisAppid, topic, params)

		writeBadRequest(w, "invalid cluster")
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
		log.Error("app[%s] alter topic:%s %s: %+v", hisAppid, topic, query.Encode(), err)

		writeBadRequest(w, err.Error())
		return
	}

	log.Info("app[%s] from %s(%s) alter topic: {appid:%s cluster:%s topic:%s ver:%s query:%s}",
		appid, r.RemoteAddr, realIp, hisAppid, cluster, topic, ver, query.Encode())

	rawTopic := manager.Default.KafkaTopic(hisAppid, topic, ver)
	alterConfig := ts.DumpForAlterTopic()
	if len(alterConfig) == 0 {
		log.Warn("app[%s] from %s(%s) alter topic: {appid:%s cluster:%s topic:%s ver:%s query:%s} nothing updated",
			appid, r.RemoteAddr, realIp, hisAppid, cluster, topic, ver, query.Encode())

		writeBadRequest(w, "nothing updated")
		return
	}

	lines, err := zkcluster.AlterTopic(rawTopic, ts)
	if err != nil {
		log.Error("app[%s] from %s(%s) alter topic: {appid:%s cluster:%s topic:%s ver:%s query:%s} %v",
			appid, r.RemoteAddr, realIp, hisAppid, cluster, topic, ver, query.Encode(), err)

		writeServerError(w, err.Error())
		return
	}

	for _, l := range lines {
		log.Trace("app[%s] alter topic[%s] in cluster %s: %s", appid, rawTopic, cluster, l)
	}

	w.Write(ResponseOk)
}

// @rest DELETE /v1/manager/cache
func (this *manServer) refreshManagerHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	appid := r.Header.Get(HttpHeaderAppid)
	pubkey := r.Header.Get(HttpHeaderPubkey)
	realIp := getHttpRemoteIp(r)

	if !manager.Default.AuthAdmin(appid, pubkey) {
		log.Warn("suspicous refresh call from %s(%s) {app:%s key:%s}",
			r.RemoteAddr, realIp, appid, pubkey)

		writeAuthFailure(w, manager.ErrAuthenticationFail)
		return
	}

	if !this.throttleAddTopic.Pour(realIp, 1) {
		writeQuotaExceeded(w)
		return
	}

	kateways, err := this.gw.zkzone.KatewayInfos()
	if err != nil {
		log.Error("refresh from %s(%s) %v", r.RemoteAddr, getHttpRemoteIp(r), err)

		writeServerError(w, err.Error())
		return
	}

	// refresh locally
	manager.Default.ForceRefresh()

	// refresh zone wide
	allOk := true
	for _, kw := range kateways {
		if kw.Id != this.gw.id {
			// notify other kateways to refresh: avoid dead loop in the network
			if err := this.gw.callKateway(kw, "PUT", "v1/options/refreshdb/true"); err != nil {
				// don't retry, just log
				log.Error("refresh from %s(%s) %s@%s: %v", r.RemoteAddr, realIp, kw.Id, kw.Host, err)

				allOk = false
			}
		}
	}

	log.Info("refresh from %s(%s) all ok: %v", r.RemoteAddr, realIp, allOk)

	if !allOk {
		writeServerError(w, "cache partially refreshed")
		return
	}

	w.Write(ResponseOk)
}
