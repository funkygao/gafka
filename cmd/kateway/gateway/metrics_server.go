package gateway

import (
	"encoding/json"
	"expvar"
	"runtime"
	"time"

	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
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
	gw *Gateway

	TotalConns      metrics.Counter
	ConcurrentConns metrics.Counter
	ConcurrentPub   metrics.Counter
	ConcurrentSub   metrics.Counter
	ConcurrentSubWs metrics.Counter
}

func NewServerMetrics(interval time.Duration, gw *Gateway) *serverMetrics {
	this := &serverMetrics{
		gw:              gw,
		TotalConns:      metrics.NewRegisteredCounter("server.totalconns", metrics.DefaultRegistry),
		ConcurrentConns: metrics.NewRegisteredCounter("server.concurrent", metrics.DefaultRegistry),
		ConcurrentPub:   metrics.NewRegisteredCounter("server.conns.pub", metrics.DefaultRegistry),
		ConcurrentSub:   metrics.NewRegisteredCounter("server.conns.sub", metrics.DefaultRegistry),
		ConcurrentSubWs: metrics.NewRegisteredCounter("server.conns.subws", metrics.DefaultRegistry),
	}

	if Options.DebugHttpAddr != "" {
		expvar.Publish("Goroutines", expvar.Func(goroutines))
	}

	return this
}

func (this *serverMetrics) Key() string {
	return "server"
}

func (this *serverMetrics) Load() {
	b, err := this.gw.zkzone.LoadKatewayMetrics(this.gw.id, this.Key())
	if err != nil {
		log.Error("load %s metrics: %v", this.Key(), err)
		return
	}

	data := make(map[string]int64)
	json.Unmarshal(b, &data)
	this.TotalConns.Inc(data["total"])
}

func (this *serverMetrics) Flush() {
	var data = map[string]int64{
		"total": this.TotalConns.Count(),
	}
	b, _ := json.Marshal(data)
	this.gw.zkzone.FlushKatewayMetrics(this.gw.id, this.Key(), b)
}
