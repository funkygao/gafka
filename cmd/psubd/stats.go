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
	numGo     metrics.Gauge
	gcNum     metrics.Gauge
	gcPause   metrics.Gauge
	heapAlloc metrics.Gauge
	qps       metrics.Meter
	latency   metrics.Timer
}

func newPubMetrics() *pubMetrics {
	this := &pubMetrics{
		numGo: metrics.NewGauge(),
	}

	metrics.Register("gc.num", this.gcNum)
	metrics.Register("gc.pause", this.gcPause)
	metrics.Register("gc.heap", this.heapAlloc)
	metrics.Register("req.qps", this.qps)
	metrics.Register("req.latency", this.latency)

	// stdout reporter
	go metrics.Log(metrics.DefaultRegistry, options.tick,
		log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
	// influxdb reporter
	if false {
		go influxdb.InfluxDB(metrics.DefaultRegistry, options.tick,
			"http://localhost:8086", "pubsub", "", "")
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

			this.numGo.Update(int64(runtime.NumGoroutine()))
			this.gcNum.Update(int64(mem.NumGC))
			this.heapAlloc.Update(int64(mem.HeapAlloc))
			this.gcPause.Update(int64(mem.PauseTotalNs - lastTotalGcPause))
			lastTotalGcPause = mem.PauseTotalNs

		}
	}
}
