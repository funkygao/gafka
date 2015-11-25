package main

import (
	"log"
	"os"
	"runtime"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/vrischmann/go-metrics-influxdb"
)

type pubMetrics struct {
	NumGo       metrics.Gauge
	GcNum       metrics.Gauge
	GcPause     metrics.Gauge
	HeapAlloc   metrics.Gauge
	HeapObjects metrics.Gauge
	PubSize     metrics.Meter
	PubQps      metrics.Meter
	PubLatency  metrics.Histogram
}

func newPubMetrics() *pubMetrics {
	this := new(pubMetrics)
	this.NumGo = metrics.NewGauge()
	this.GcNum = metrics.NewGauge()
	this.GcPause = metrics.NewGauge()
	this.HeapAlloc = metrics.NewGauge()
	this.HeapObjects = metrics.NewGauge()
	this.PubQps = metrics.NewMeter()
	this.PubSize = metrics.NewMeter()
	this.PubLatency = metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015))

	metrics.Register("sys.gc.num", this.GcNum)
	metrics.Register("sys.gc.pause.ns", this.GcPause)    // in ns
	metrics.Register("sys.gc.heap.byte", this.HeapAlloc) // in byte
	metrics.Register("sys.gc.heap.objects", this.HeapObjects)
	metrics.Register("sys.go.num", this.NumGo)
	metrics.Register("pub.qps", this.PubQps)
	metrics.Register("pub.size", this.PubSize)       // pub msg size
	metrics.Register("pub.latency", this.PubLatency) // in ms

	// stdout reporter
	go metrics.Log(metrics.DefaultRegistry, options.tick,
		log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
	// influxdb reporter
	if options.influxServer != "" {
		go influxdb.InfluxDB(metrics.DefaultRegistry, options.tick,
			options.influxServer, "psub", "", "")
	}

	go this.mainLoop()
	return this
}

func (this *pubMetrics) mainLoop() {
	ticker := time.NewTicker(options.tick)
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

		}
	}
}
