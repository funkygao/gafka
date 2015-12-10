package main

import (
	"log"
	"os"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/rcrowley/go-metrics"
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
	// BytesInPerSec, BytesOutPerSec, FailedMessagesPerSec
	PubSuccess    metrics.Counter
	PubFailure    metrics.Counter
	ClientError   metrics.Counter
	PubConcurrent metrics.Counter
	PubQps        metrics.Meter
	PubLatency    metrics.Histogram
	PubMsgSize    metrics.Histogram
}

func newPubMetrics(interval time.Duration) *pubMetrics {
	this := &pubMetrics{
		PubConcurrent: metrics.NewRegisteredCounter("pub.concurrent.count", metrics.DefaultRegistry),
		PubFailure:    metrics.NewRegisteredCounter("pub.fail.count", metrics.DefaultRegistry),
		PubSuccess:    metrics.NewRegisteredCounter("pub.ok.count", metrics.DefaultRegistry),
		ClientError:   metrics.NewRegisteredCounter("pub.clienterr.count", metrics.DefaultRegistry),
		PubQps:        metrics.NewRegisteredMeter("pub.qps", metrics.DefaultRegistry),
		PubMsgSize:    metrics.NewRegisteredHistogram("pub.msgsize", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
		PubLatency:    metrics.NewRegisteredHistogram("pub.latency", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015)),
	}

	// stdout reporter
	go metrics.Log(metrics.DefaultRegistry, interval*60,
		log.New(os.Stdout, "", log.Lmicroseconds))

	// influxdb reporter
	if options.influxServer != "" {
		go InfluxDB(ctx.Hostname(), metrics.DefaultRegistry, interval,
			options.influxServer, "kateway1", "", "") // FIXME
	}

	return this
}
