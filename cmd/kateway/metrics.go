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

	// mem general
	MemLookups metrics.Gauge
	MemMallocs metrics.Gauge
	MemFrees   metrics.Gauge

	// mem heap
	HeapAlloc    metrics.Gauge
	HeapObjects  metrics.Gauge
	HeapReleased metrics.Gauge

	// mem stack
	StackSys   metrics.Gauge
	StackInUse metrics.Gauge

	// mem gc
	GcNum   metrics.Gauge
	GcPause metrics.Gauge
	GcSys   metrics.Gauge
	GcLast  metrics.Gauge
	GcNext  metrics.Gauge

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

		NumGoroutine: metrics.NewGauge(),
		NumCgoCall:   metrics.NewGauge(),

		MemLookups: metrics.NewGauge(),
		MemMallocs: metrics.NewGauge(),
		MemFrees:   metrics.NewGauge(),

		GcNum:   metrics.NewGauge(),
		GcPause: metrics.NewGauge(),
		GcSys:   metrics.NewGauge(),
		GcLast:  metrics.NewGauge(),
		GcNext:  metrics.NewGauge(),

		HeapAlloc:    metrics.NewGauge(),
		HeapObjects:  metrics.NewGauge(),
		HeapReleased: metrics.NewGauge(),

		StackSys:   metrics.NewGauge(),
		StackInUse: metrics.NewGauge(),

		PubConcurrent: metrics.NewCounter(),
		PubFailure:    metrics.NewCounter(),
		PubSuccess:    metrics.NewCounter(),
		ClientError:   metrics.NewCounter(),
		PubQps:        metrics.NewMeter(),
		PubSize:       metrics.NewMeter(),
		PubLatency:    metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015)),
	}

	// runtime cpu
	metrics.Register("runtime.goroutines", this.NumGoroutine) // number of goroutines
	metrics.Register("runtime.cgo_call", this.NumCgoCall)

	// mem general
	metrics.Register("mem.lookups", this.MemLookups)
	metrics.Register("mem.mallocs", this.MemMallocs)
	metrics.Register("mem.frees", this.MemFrees)

	// mem heap
	metrics.Register("mem.heap.byte", this.HeapAlloc)      // in byte
	metrics.Register("mem.heap.objects", this.HeapObjects) // heap objects
	metrics.Register("mem.heap.released", this.HeapReleased)

	// mem stack
	metrics.Register("mem.stack.sys", this.StackSys)
	metrics.Register("mem.stack.inuse", this.StackInUse)

	// mem gc
	metrics.Register("mem.gc.count", this.GcNum)
	metrics.Register("mem.gc.pause.ns", this.GcPause) // in ns
	metrics.Register("mem.gc.sys", this.GcSys)
	metrics.Register("mem.gc.last", this.GcLast)
	metrics.Register("mem.gc.next", this.GcNext)

	// pub
	metrics.Register("pub.clients.count", this.PubConcurrent) // concurrent pub conns
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

	mem := new(runtime.MemStats)
	var lastTotalGcPause uint64
	for {
		select {
		case <-ticker.C:
			// runtime cpu
			this.NumGoroutine.Update(int64(runtime.NumGoroutine()))
			this.NumCgoCall.Update(runtime.NumCgoCall())

			runtime.ReadMemStats(mem)

			// mem general
			this.MemLookups.Update(int64(mem.Lookups))
			this.MemMallocs.Update(int64(mem.Mallocs))
			this.MemFrees.Update(int64(mem.Frees))

			// mem heap
			this.HeapAlloc.Update(int64(mem.HeapAlloc))
			this.HeapObjects.Update(int64(mem.HeapObjects))
			this.HeapReleased.Update(int64(mem.HeapReleased))

			// mem stack
			this.StackSys.Update(int64(mem.StackSys))
			this.StackInUse.Update(int64(mem.StackInuse))

			// mem gc
			this.GcNum.Update(int64(mem.NumGC))
			this.GcLast.Update(int64(mem.LastGC))
			this.GcNext.Update(int64(mem.NextGC))
			this.GcSys.Update(int64(mem.GCSys))
			this.GcPause.Update(int64(mem.PauseTotalNs - lastTotalGcPause))
			lastTotalGcPause = mem.PauseTotalNs

		case <-this.gw.shutdownCh:
			log4go.Trace("metrics reporter stopped")
			return
		}
	}
}
