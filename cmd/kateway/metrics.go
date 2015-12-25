package main

import (
	"expvar"
	"runtime"
	"sync"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-metrics"
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
	TotalConns      metrics.Counter
	ConcurrentConns metrics.Counter
}

func NewServerMetrics(interval time.Duration) *serverMetrics {
	this := &serverMetrics{
		TotalConns:      metrics.NewRegisteredCounter("server.totalconns", metrics.DefaultRegistry),
		ConcurrentConns: metrics.NewRegisteredCounter("server.concurrent", metrics.DefaultRegistry),
	}

	if options.debugHttpAddr != "" {
		expvar.Publish("Goroutines", expvar.Func(goroutines))
	}

	go runMetricsReporter(metrics.DefaultRegistry, interval*60)

	// influxdb reporter
	if options.influxServer != "" {
		go InfluxDB(ctx.Hostname(), metrics.DefaultRegistry, interval,
			options.influxServer, "kateway1", "", "") // FIXME
	}

	return this
}

type subMetrics struct {
	expConsumeOk      *expvar.Int
	expActiveConns    *expvar.Int
	expActiveUpstream *expvar.Int

	// i consume msgs of others
	ConsumeMap   map[string]metrics.Counter
	consumeMapMu sync.RWMutex

	// my msgs are consumed by others
	// TODO who are consuming my msgs
	ConsumedMap   map[string]metrics.Counter
	consumedMapMu sync.RWMutex
}

func NewSubMetrics() *subMetrics {
	this := &subMetrics{
		ConsumeMap:  make(map[string]metrics.Counter),
		ConsumedMap: make(map[string]metrics.Counter),
	}

	if options.debugHttpAddr != "" {
		this.expConsumeOk = expvar.NewInt("ConsumeOk")
		this.expActiveConns = expvar.NewInt("ConsumeConns")       // TODO
		this.expActiveUpstream = expvar.NewInt("ConsumeUpstream") // TODO
	}

	return this
}

func (this *subMetrics) ConsumeOk(appid, topic, ver string) {
	if this.expConsumeOk != nil {
		this.expConsumeOk.Add(1)
	}
	updateCounter(appid, topic, ver, "sub.ok", &this.consumeMapMu, this.ConsumeMap)
}

func (this *subMetrics) ConsumedOk(appid, topic, ver string) {
	updateCounter(appid, topic, ver, "subd.ok", &this.consumedMapMu, this.ConsumedMap)
}

type pubMetrics struct {
	expPubOk          *expvar.Int
	expPubFail        *expvar.Int
	expActiveConns    *expvar.Int
	expActiveUpstream *expvar.Int

	PubOkMap   map[string]metrics.Counter
	pubOkMu    sync.RWMutex
	PubFailMap map[string]metrics.Counter
	pubFailMu  sync.RWMutex

	// BytesInPerSec, BytesOutPerSec, FailedMessagesPerSec
	ClientError metrics.Counter
	PubQps      metrics.Meter
	PubLatency  metrics.Histogram
	PubMsgSize  metrics.Histogram
}

func NewPubMetrics() *pubMetrics {
	this := &pubMetrics{
		PubOkMap:   make(map[string]metrics.Counter),
		PubFailMap: make(map[string]metrics.Counter),

		ClientError: metrics.NewRegisteredCounter("pub.clienterr", metrics.DefaultRegistry),
		PubQps:      metrics.NewRegisteredMeter("pub.qps", metrics.DefaultRegistry),
		PubMsgSize:  metrics.NewRegisteredHistogram("pub.msgsize", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
		PubLatency:  metrics.NewRegisteredHistogram("pub.latency", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
	}

	if options.debugHttpAddr != "" {
		this.expPubOk = expvar.NewInt("PubOk")
		this.expPubFail = expvar.NewInt("PubFail")
		this.expActiveConns = expvar.NewInt("PubConns")       // TODO
		this.expActiveUpstream = expvar.NewInt("PubUpstream") // TODO
	}

	return this
}

func (this *pubMetrics) pubFail(appid, topic, ver string) {
	if this.expPubFail != nil {
		this.expPubFail.Add(1)
	}
	updateCounter(appid, topic, ver, "pub.fail", &this.pubFailMu, this.PubFailMap)
}

func (this *pubMetrics) pubOk(appid, topic, ver string) {
	if this.expPubOk != nil {
		this.expPubOk.Add(1)
	}
	updateCounter(appid, topic, ver, "pub.ok", &this.pubOkMu, this.PubOkMap)
}
