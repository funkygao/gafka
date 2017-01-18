package gateway

import (
	"encoding/json"
	"expvar"
	"sync"

	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

type pubMetrics struct {
	gw *Gateway

	expPubOk          *expvar.Int
	expPubFail        *expvar.Int
	expActiveConns    *expvar.Int
	expActiveUpstream *expvar.Int

	// multi-tenant related
	PubOkMap   map[string]metrics.Counter
	pubOkMu    sync.RWMutex
	PubFailMap map[string]metrics.Counter
	pubFailMu  sync.RWMutex

	InternalErr metrics.Counter
	ClientError metrics.Counter
	PubQps      metrics.Meter
	PubTryQps   metrics.Meter
	JobQps      metrics.Meter
	JobTryQps   metrics.Meter
	PubLatency  metrics.Histogram
	PubMsgSize  metrics.Histogram
	JobMsgSize  metrics.Histogram
}

func NewPubMetrics(gw *Gateway) *pubMetrics {
	this := &pubMetrics{
		gw:         gw,
		PubOkMap:   make(map[string]metrics.Counter),
		PubFailMap: make(map[string]metrics.Counter),

		InternalErr: metrics.NewRegisteredCounter("pub.internalerr", metrics.DefaultRegistry),
		ClientError: metrics.NewRegisteredCounter("pub.clienterr", metrics.DefaultRegistry),
		PubQps:      metrics.NewRegisteredMeter("pub.qps", metrics.DefaultRegistry),
		PubTryQps:   metrics.NewRegisteredMeter("pub.try.qps", metrics.DefaultRegistry),
		JobQps:      metrics.NewRegisteredMeter("job.qps", metrics.DefaultRegistry),
		JobTryQps:   metrics.NewRegisteredMeter("job.try.qps", metrics.DefaultRegistry),
		PubMsgSize:  metrics.NewRegisteredHistogram("pub.msgsize", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
		JobMsgSize:  metrics.NewRegisteredHistogram("job.msgsize", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
		PubLatency:  metrics.NewRegisteredHistogram("pub.latency", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
	}

	if Options.DebugHttpAddr != "" {
		this.expPubOk = expvar.NewInt("PubOk")
		this.expPubFail = expvar.NewInt("PubFail")
		this.expActiveConns = expvar.NewInt("PubConns")       // TODO
		this.expActiveUpstream = expvar.NewInt("PubUpstream") // TODO
	}

	return this
}

func (this *pubMetrics) Key() string {
	return "pub"
}

// TODO need test
func (this *pubMetrics) Load() {
	b, err := this.gw.zkzone.LoadKatewayMetrics(this.gw.id, this.Key())
	if err != nil {
		log.Error("load %s metrics: %v", this.Key(), err)
		return
	}

	data := make(map[string]map[string]int64)
	json.Unmarshal(b, &data)

	for k, v := range data["ok"] {
		if _, present := this.PubOkMap[k]; !present {
			this.PubOkMap[k] = metrics.NewRegisteredCounter(k+"pub.ok", metrics.DefaultRegistry)
		}
		this.PubOkMap[k].Inc(v)
	}
	for k, v := range data["fail"] {
		if _, present := this.PubFailMap[k]; !present {
			this.PubFailMap[k] = metrics.NewRegisteredCounter(k+"pub.fail", metrics.DefaultRegistry)
		}
		this.PubFailMap[k].Inc(v)
	}
}

func (this *pubMetrics) Flush() {
	var data = make(map[string]map[string]int64)
	data["ok"] = make(map[string]int64)
	data["fail"] = make(map[string]int64)
	for k, v := range this.PubOkMap {
		data["ok"][k] = v.Count()
	}
	for k, v := range this.PubFailMap {
		data["fail"][k] = v.Count()
	}

	b, _ := json.Marshal(data)
	this.gw.zkzone.FlushKatewayMetrics(this.gw.id, this.Key(), b)
}

func (this *pubMetrics) PubFail(appid, topic, ver string) {
	if this.expPubFail != nil {
		this.expPubFail.Add(1)
	}
	telemetry.UpdateCounter(appid, topic, ver, "pub.fail", 1, &this.pubFailMu, this.PubFailMap)
}

func (this *pubMetrics) PubOk(appid, topic, ver string) {
	if this.expPubOk != nil {
		this.expPubOk.Add(1)
	}
	telemetry.UpdateCounter(appid, topic, ver, "pub.ok", 1, &this.pubOkMu, this.PubOkMap)
}
