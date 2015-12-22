package main

import (
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
	ClientError metrics.Counter
}

func newSubMetrics(interval time.Duration) *subMetrics {
	this := &subMetrics{
		ClientError: metrics.NewCounter(),
	}
	return this
}

type pubMetrics struct {
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

	go runMetricsReporter(metrics.DefaultRegistry, interval*60)

	// influxdb reporter
	if options.influxServer != "" {
		go InfluxDB(ctx.Hostname(), metrics.DefaultRegistry, interval,
			options.influxServer, "kateway1", "", "") // FIXME
	}

	return this
}

func (this *pubMetrics) updateCounter(appid, topic, ver, name string,
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
	this.updateCounter(appid, topic, ver, "pub.fail", &this.pubFailMu, this.PubFailMap)
}

func (this *pubMetrics) pubOk(appid, topic, ver string) {
	this.updateCounter(appid, topic, ver, "pub.ok", &this.pubOkMu, this.PubOkMap)
}
