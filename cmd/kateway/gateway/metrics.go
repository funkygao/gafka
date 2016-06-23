package gateway

import (
	"encoding/json"
	"expvar"
	"runtime"
	"sync"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func startRuntimeMetrics(interval time.Duration) {
	metrics.RegisterDebugGCStats(metrics.DefaultRegistry)
	metrics.RegisterRuntimeMemStats(metrics.DefaultRegistry)

	go metrics.CaptureDebugGCStats(metrics.DefaultRegistry, interval)
	go metrics.CaptureRuntimeMemStats(metrics.DefaultRegistry, interval)
}

func goroutines() interface{} {
	return runtime.NumGoroutine()
}

type serverMetrics struct {
	gw *Gateway

	TotalConns      metrics.Counter
	ConcurrentConns metrics.Counter
	ConcurrentPub   metrics.Counter
	ConcurrentSub   metrics.Counter
	ConcurrentSubWs metrics.Counter
}

func NewServerMetrics(interval time.Duration, gw *Gateway) *serverMetrics {
	this := &serverMetrics{
		gw:              gw,
		TotalConns:      metrics.NewRegisteredCounter("server.totalconns", metrics.DefaultRegistry),
		ConcurrentConns: metrics.NewRegisteredCounter("server.concurrent", metrics.DefaultRegistry),
		ConcurrentPub:   metrics.NewRegisteredCounter("server.conns.pub", metrics.DefaultRegistry),
		ConcurrentSub:   metrics.NewRegisteredCounter("server.conns.sub", metrics.DefaultRegistry),
		ConcurrentSubWs: metrics.NewRegisteredCounter("server.conns.subws", metrics.DefaultRegistry),
	}

	if Options.DebugHttpAddr != "" {
		expvar.Publish("Goroutines", expvar.Func(goroutines))
	}

	if Options.ConsoleMetricsInterval > 0 {
		go runMetricsReporter(metrics.DefaultRegistry, Options.ConsoleMetricsInterval)
	}

	// influxdb reporter
	if Options.InfluxServer != "" {
		go InfluxDB(ctx.Hostname(), metrics.DefaultRegistry, interval,
			Options.InfluxServer, Options.InfluxDbName, "", "")
	}

	return this
}

func (this *serverMetrics) Key() string {
	return "server"
}

func (this *serverMetrics) Load() {
	b, err := this.gw.zkzone.LoadKatewayMetrics(this.gw.id, this.Key())
	if err != nil {
		log.Error("load %s metrics: %v", this.Key(), err)
		return
	}

	data := make(map[string]int64)
	json.Unmarshal(b, &data)
	this.TotalConns.Inc(data["total"])
}

func (this *serverMetrics) Flush() {
	var data = map[string]int64{
		"total": this.TotalConns.Count(),
	}
	b, _ := json.Marshal(data)
	this.gw.zkzone.FlushKatewayMetrics(this.gw.id, this.Key(), b)
}

type subMetrics struct {
	gw *Gateway

	expConsumeOk      *expvar.Int
	expActiveConns    *expvar.Int
	expActiveUpstream *expvar.Int

	// multi-tenant related
	ConsumeMap    map[string]metrics.Counter // i consume msgs of others
	consumeMapMu  sync.RWMutex
	ConsumedMap   map[string]metrics.Counter // my msgs are consumed by others
	consumedMapMu sync.RWMutex               // TODO who are consuming my msgs
}

func NewSubMetrics(gw *Gateway) *subMetrics {
	this := &subMetrics{
		gw:          gw,
		ConsumeMap:  make(map[string]metrics.Counter),
		ConsumedMap: make(map[string]metrics.Counter),
	}

	if Options.DebugHttpAddr != "" {
		this.expConsumeOk = expvar.NewInt("ConsumeOk")
		this.expActiveConns = expvar.NewInt("ConsumeConns")       // TODO
		this.expActiveUpstream = expvar.NewInt("ConsumeUpstream") // TODO
	}

	return this
}

func (this *subMetrics) Key() string {
	return "sub"
}

func (this *subMetrics) Load() {
	b, err := this.gw.zkzone.LoadKatewayMetrics(this.gw.id, this.Key())
	if err != nil {
		log.Warn("load %s metrics: %v", this.Key(), err)
		return
	}

	data := make(map[string]map[string]int64)
	json.Unmarshal(b, &data)

	for k, v := range data["sub"] {
		if _, present := this.ConsumeMap[k]; !present {
			this.ConsumeMap[k] = metrics.NewRegisteredCounter(k+"sub.ok", metrics.DefaultRegistry)
		}
		this.ConsumeMap[k].Inc(v)
	}
	for k, v := range data["subd"] {
		if _, present := this.ConsumedMap[k]; !present {
			this.ConsumedMap[k] = metrics.NewRegisteredCounter(k+"subd.ok", metrics.DefaultRegistry)
		}
		this.ConsumedMap[k].Inc(v)
	}
}

func (this *subMetrics) Flush() {
	var data = make(map[string]map[string]int64)
	data["sub"] = make(map[string]int64)
	data["subd"] = make(map[string]int64)
	for k, v := range this.ConsumeMap {
		data["sub"][k] = v.Count()
	}
	for k, v := range this.ConsumedMap {
		data["subd"][k] = v.Count()
	}

	b, _ := json.Marshal(data)
	this.gw.zkzone.FlushKatewayMetrics(this.gw.id, this.Key(), b)
}

func (this *subMetrics) ConsumeOk(appid, topic, ver string) {
	if this.expConsumeOk != nil {
		this.expConsumeOk.Add(1)
	}
	updateCounter(appid, topic, ver, "sub.ok", 1, &this.consumeMapMu, this.ConsumeMap)
}

func (this *subMetrics) ConsumedOk(appid, topic, ver string) {
	updateCounter(appid, topic, ver, "subd.ok", 1, &this.consumedMapMu, this.ConsumedMap)
}

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

	ClientError metrics.Counter
	PubQps      metrics.Meter // FIXME if 2 servers run on 1 host, the metrics will be wrong
	PubLatency  metrics.Histogram
	PubMsgSize  metrics.Histogram
}

func NewPubMetrics(gw *Gateway) *pubMetrics {
	this := &pubMetrics{
		gw:         gw,
		PubOkMap:   make(map[string]metrics.Counter),
		PubFailMap: make(map[string]metrics.Counter),

		ClientError: metrics.NewRegisteredCounter("pub.clienterr", metrics.DefaultRegistry),
		PubQps:      metrics.NewRegisteredMeter("pub.qps", metrics.DefaultRegistry),
		PubMsgSize:  metrics.NewRegisteredHistogram("pub.msgsize", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
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
	updateCounter(appid, topic, ver, "pub.fail", 1, &this.pubFailMu, this.PubFailMap)
}

func (this *pubMetrics) PubOk(appid, topic, ver string) {
	if this.expPubOk != nil {
		this.expPubOk.Add(1)
	}
	updateCounter(appid, topic, ver, "pub.ok", 1, &this.pubOkMu, this.PubOkMap)
}
