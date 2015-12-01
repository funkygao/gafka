package main

import (
	"log"
	"os"
	"runtime"
	"time"

	"github.com/funkygao/log4go"
	"github.com/rcrowley/go-metrics"
)

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

// TODO add tag  hostname=this.hostname
type pubMetrics struct {
	gw *Gateway

	NumGo       metrics.Gauge
	GcNum       metrics.Gauge
	GcPause     metrics.Gauge
	HeapAlloc   metrics.Gauge
	HeapObjects metrics.Gauge

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

		NumGo:         metrics.NewGauge(),
		GcNum:         metrics.NewGauge(),
		GcPause:       metrics.NewGauge(),
		PubConcurrent: metrics.NewCounter(),
		PubFailure:    metrics.NewCounter(),
		PubSuccess:    metrics.NewCounter(),
		ClientError:   metrics.NewCounter(),
		HeapAlloc:     metrics.NewGauge(),
		HeapObjects:   metrics.NewGauge(),
		PubQps:        metrics.NewMeter(),
		PubSize:       metrics.NewMeter(),
		PubLatency:    metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
	}

	metrics.Register("sys.gc.num", this.GcNum)
	metrics.Register("sys.gc.pause.ns", this.GcPause)         // in ns
	metrics.Register("sys.gc.heap.byte", this.HeapAlloc)      // in byte
	metrics.Register("sys.gc.heap.objects", this.HeapObjects) // heap objects
	metrics.Register("sys.go.num", this.NumGo)                // number of goroutines
	metrics.Register("pub.clients.num", this.PubConcurrent)   // concurrent pub conns
	metrics.Register("pub.num.write.fail", this.ClientError)  // client conn broken
	metrics.Register("pub.num.ok", this.PubSuccess)           // pub accumulated success
	metrics.Register("pub.num.fail", this.PubFailure)         // pub accumulated failures
	metrics.Register("pub.qps", this.PubQps)                  // pub qps
	metrics.Register("pub.size", this.PubSize)                // pub msg size
	metrics.Register("pub.latency", this.PubLatency)          // in ms

	// stdout reporter
	go metrics.Log(metrics.DefaultRegistry, options.reporterInterval*60,
		log.New(os.Stdout, "", log.Lmicroseconds))

	// influxdb reporter
	if options.influxServer != "" {
		go InfluxDB(this.gw.hostname, metrics.DefaultRegistry, options.reporterInterval,
			options.influxServer, "kateway", "", "")
	}

	go this.mainLoop()
	return this
}

func (this *pubMetrics) mainLoop() {
	log4go.Info("metrics reporter started")

	ticker := time.NewTicker(options.reporterInterval)
	mem := new(runtime.MemStats)
	var lastTotalGcPause uint64
	for {
		select {
		case <-ticker.C:
			runtime.ReadMemStats(mem)

			this.NumGo.Update(int64(runtime.NumGoroutine()))
			this.GcNum.Update(int64(mem.NumGC))
			this.HeapAlloc.Update(int64(mem.HeapAlloc))
			this.HeapObjects.Update(int64(mem.HeapObjects))
			this.GcPause.Update(int64(mem.PauseTotalNs - lastTotalGcPause))
			lastTotalGcPause = mem.PauseTotalNs

		case <-this.gw.shutdownCh:
			ticker.Stop()

		}
	}
}
