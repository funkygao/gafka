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

type subMetrics struct {
	expConsumeOk *expvar.Int

	ClientError metrics.Counter

	// i consume msgs of others
	ConsumeMap   map[string]metrics.Counter
	consumeMapMu sync.RWMutex

	// my msgs are consumed by others
	// TODO who are consuming my msgs
	ConsumedMap   map[string]metrics.Counter
	consumedMapMu sync.RWMutex
}

func newSubMetrics(interval time.Duration) *subMetrics {
	this := &subMetrics{
		ClientError: metrics.NewCounter(),
		ConsumeMap:  make(map[string]metrics.Counter),
		ConsumedMap: make(map[string]metrics.Counter),
	}

	if options.debugHttpAddr != "" {
		this.expConsumeOk = expvar.NewInt("ConsumeOk")
	}
	return this
}

func (this *subMetrics) consumeOk(appid, topic, ver string) {
	if this.expConsumeOk != nil {
		this.expConsumeOk.Add(1)
	}
	updateCounter(appid, topic, ver, "sub.ok", &this.consumeMapMu, this.ConsumeMap)
}

func (this *subMetrics) consumedOk(appid, topic, ver string) {
	updateCounter(appid, topic, ver, "subd.ok", &this.consumedMapMu, this.ConsumedMap)
}

func goroutines() interface{} {
	return runtime.NumGoroutine()
}

type pubMetrics struct {
	expPubOk   *expvar.Int
	expPubFail *expvar.Int

	PubOkMap   map[string]metrics.Counter
	pubOkMu    sync.RWMutex
	PubFailMap map[string]metrics.Counter
	pubFailMu  sync.RWMutex

	// BytesInPerSec, BytesOutPerSec, FailedMessagesPerSec
	ConnAccept    metrics.Counter
	ClientError   metrics.Counter
	PubConcurrent metrics.Counter
	PubQps        metrics.Meter
	PubLatency    metrics.Histogram
	PubMsgSize    metrics.Histogram // FIXME
}

func newPubMetrics(interval time.Duration) *pubMetrics {
	this := &pubMetrics{
		PubOkMap:   make(map[string]metrics.Counter),
		PubFailMap: make(map[string]metrics.Counter),

		ConnAccept:    metrics.NewRegisteredCounter("pub.accept", metrics.DefaultRegistry),
		PubConcurrent: metrics.NewRegisteredCounter("pub.concurrent", metrics.DefaultRegistry),
		ClientError:   metrics.NewRegisteredCounter("pub.clienterr", metrics.DefaultRegistry),
		PubQps:        metrics.NewRegisteredMeter("pub.qps", metrics.DefaultRegistry),
		PubMsgSize:    metrics.NewRegisteredHistogram("pub.msgsize", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
		PubLatency:    metrics.NewRegisteredHistogram("pub.latency", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
	}
	if options.debugHttpAddr != "" {
		this.expPubOk = expvar.NewInt("PubOk")
		this.expPubFail = expvar.NewInt("PubFail")
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

func updateCounter(appid, topic, ver, name string,
	mu *sync.RWMutex, m map[string]metrics.Counter) {
	tagBuf := make([]byte, 4+len(appid)+len(topic)+len(ver))
	tagBuf[0] = CharBraceletLeft
	idx := 1
	for ; idx <= len(appid); idx++ {
		tagBuf[idx] = appid[idx-1]
	}
	tagBuf[idx] = CharDot
	idx++
	for j := 0; j < len(topic); j++ {
		tagBuf[idx+j] = topic[j]
	}
	idx += len(topic)
	tagBuf[idx] = CharDot
	idx++
	for j := 0; j < len(ver); j++ {
		tagBuf[idx+j] = ver[j]
	}
	idx += len(ver)
	tagBuf[idx] = CharBraceletRight

	mu.RLock()
	// golang has optimization avoids extra allocations when []byte keys are used to
	// lookup entries in map[string] collections: m[string(key)]
	counter, present := m[string(tagBuf)]
	mu.RUnlock()

	if present {
		counter.Inc(1)
		return
	}

	// seldom goes here, needn't optimize

	tag := string(tagBuf)
	mu.Lock()
	m[tag] = metrics.NewRegisteredCounter(tag+name, metrics.DefaultRegistry)
	mu.Unlock()

	m[tag].Inc(1)
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
