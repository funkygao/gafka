package main

import (
	"log"
	"os"
	"runtime"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/log4go"
	"github.com/rcrowley/go-metrics"
)

// TODO cpu stats
func startRuntimeMetrics(interval time.Duration) {
	metrics.RegisterDebugGCStats(metrics.DefaultRegistry)
	metrics.RegisterRuntimeMemStats(metrics.DefaultRegistry)

	go metrics.CaptureDebugGCStats(metrics.DefaultRegistry, interval)
	go metrics.CaptureRuntimeMemStats(metrics.DefaultRegistry, interval)
}

type subMetrics struct {
	gw *Gateway

	ClientError metrics.Counter
}

func newSubMetrics(gw *Gateway) *subMetrics {
	this := &subMetrics{
		gw: gw,

		ClientError: metrics.NewCounter(),
	}
	return this
}

type pubMetrics struct {
	gw *Gateway

	// runtime cpu
	NumGoroutine metrics.Gauge
	NumCgoCall   metrics.Gauge

	// pub
	PubSuccess    metrics.Counter
	PubFailure    metrics.Counter
	ClientError   metrics.Counter
	PubConcurrent metrics.Counter
	PubSize       metrics.Meter
	PubQps        metrics.Meter
	PubLatency    metrics.Histogram
}

func newPubMetrics(gw *Gateway) *pubMetrics {
	this := &pubMetrics{
		gw: gw,

		NumGoroutine: metrics.NewRegisteredGauge("runtime.goroutines", metrics.DefaultRegistry),
		NumCgoCall:   metrics.NewRegisteredGauge("runtime.cgo_call", metrics.DefaultRegistry),

		PubConcurrent: metrics.NewCounter(),
		PubFailure:    metrics.NewCounter(),
		PubSuccess:    metrics.NewCounter(),
		ClientError:   metrics.NewCounter(),
		PubQps:        metrics.NewMeter(),
		PubSize:       metrics.NewMeter(),
		PubLatency:    metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
	}

	// pub
	metrics.Register("pub.clients.count", this.PubConcurrent) // concurrent pub conns
	metrics.Register("pub.num.write.fail", this.ClientError)  // client conn broken
	metrics.Register("pub.num.ok", this.PubSuccess)           // pub accumulated success
	metrics.Register("pub.num.fail", this.PubFailure)         // pub accumulated failures
	metrics.Register("pub.qps", this.PubQps)                  // pub qps
	metrics.Register("pub.size", this.PubSize)                // pub msg size
	metrics.Register("pub.latency", this.PubLatency)          // in ms

	// stdout reporter
	go metrics.Log(metrics.DefaultRegistry, time.Second*5,
		log.New(os.Stdout, "", log.Lmicroseconds))

	// influxdb reporter
	if options.influxServer != "" {
		go InfluxDB(ctx.Hostname(), metrics.DefaultRegistry, options.reporterInterval,
			options.influxServer, "kateway", "", "")
	}

	go this.mainLoop()
	return this
}

func (this *pubMetrics) mainLoop() {
	log4go.Trace("metrics reporter started")

	ticker := time.NewTicker(options.reporterInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// runtime cpu
			this.NumGoroutine.Update(int64(runtime.NumGoroutine()))
			this.NumCgoCall.Update(runtime.NumCgoCall())

		case <-this.gw.shutdownCh:
			log4go.Trace("metrics reporter stopped")
			return
		}
	}
}
