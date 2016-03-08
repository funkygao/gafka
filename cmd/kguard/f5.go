package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

const (
	MsInNano = 1000 * 1000
	HttpLoop = 1000
)

// MonitorConsumers monitors num of online consumer groups over the time.
type MonitorF5 struct {
	stop chan struct{}
	tick time.Duration
	wg   *sync.WaitGroup

	latencyWithF5    metrics.Histogram
	latencyWithoutF5 metrics.Histogram
}

func (this *MonitorF5) Run() {
	defer this.wg.Done()

	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	this.latencyWithF5 = metrics.NewRegisteredHistogram("api.with.f5", nil, metrics.NewExpDecaySample(1028, 0.015))
	this.latencyWithoutF5 = metrics.NewRegisteredHistogram("api.without.f5", nil, metrics.NewExpDecaySample(1028, 0.015))
	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			this.callWithF5()
			this.callWithoutF5()
		}
	}
}

func (this *MonitorF5) callWithF5() {
	url := "http://api.ffan.com/pubsub/v1/pub/alive"
	this.callHttp(url, this.latencyWithF5)
}

func (this *MonitorF5) callWithoutF5() {
	url := "http://sub.sit.ffan.com:9191/alive"
	this.callHttp(url, this.latencyWithoutF5)
}

func (this *MonitorF5) callHttp(url string, h metrics.Histogram) {
	var t time.Time
	for i := 0; i < HttpLoop; i++ {
		t = time.Now()

		resp, err := http.Get(url)
		if err != nil {
			log.Error("%s %v", url, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Error("%s %s", url, resp.Status)
			continue
		}

		h.Update(time.Since(t).Nanoseconds() / MsInNano)
	}
}
