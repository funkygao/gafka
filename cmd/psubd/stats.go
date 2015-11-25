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
	NumGo      metrics.Gauge
	GcNum      metrics.Gauge
	GcPause    metrics.Gauge
	HeapAlloc  metrics.Gauge
	PubSize    metrics.Meter
	PubQps     metrics.Meter
	PubLatency metrics.Timer
}

func newPubMetrics() *pubMetrics {
	this := new(pubMetrics)
	this.NumGo = metrics.NewGauge()
	this.GcNum = metrics.NewGauge()
	this.GcPause = metrics.NewGauge()
	this.HeapAlloc = metrics.NewGauge()
	this.PubQps = metrics.NewMeter()
	this.PubSize = metrics.NewMeter()
	this.PubLatency = metrics.NewTimer()

	metrics.Register("gc.num", this.GcNum)
	metrics.Register("gc.pause", this.GcPause)
	metrics.Register("gc.heap", this.HeapAlloc)
	metrics.Register("pub.qps", this.PubQps)
	metrics.Register("pub.size", this.PubSize)
	metrics.Register("pub.latency", this.PubLatency)

	// stdout reporter
	go metrics.Log(metrics.DefaultRegistry, options.tick,
		log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
	// influxdb reporter
	if false {
		go influxdb.InfluxDB(metrics.DefaultRegistry, options.tick,
			"http://localhost:8086", "psub", "", "")
	}

	//go this.mainLoop()
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
			this.GcPause.Update(int64(mem.PauseTotalNs - lastTotalGcPause))
			lastTotalGcPause = mem.PauseTotalNs

		}
	}
}
