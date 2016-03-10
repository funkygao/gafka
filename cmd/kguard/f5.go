package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/funkygao/gafka/mpool"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

const (
	MsInNano = 1000 * 1000
	HttpLoop = 200
)

// MonitorConsumers monitors num of online consumer groups over the time.
type MonitorF5 struct {
	stop chan struct{}
	tick time.Duration
	wg   *sync.WaitGroup

	latencyWithF5WithGateway       metrics.Histogram
	latencyWithoutF5WithoutGateway metrics.Histogram
	latencyWithoutF5WithGateway    metrics.Histogram

	httpConn *http.Client
}

func (this *MonitorF5) Init() {

}

func (this *MonitorF5) Run() {
	defer this.wg.Done()

	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	this.httpConn = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1,
			DisableKeepAlives:   true, // enable http conn reuse
		},
	}

	this.latencyWithF5WithGateway = metrics.NewRegisteredHistogram("latency.api.f5yes.gwyes", nil, metrics.NewExpDecaySample(1028, 0.015))
	this.latencyWithoutF5WithoutGateway = metrics.NewRegisteredHistogram("latency.api.f5no.gwno", nil, metrics.NewExpDecaySample(1028, 0.015))
	this.latencyWithoutF5WithGateway = metrics.NewRegisteredHistogram("latency.api.f5no.gwyes", nil, metrics.NewExpDecaySample(1028, 0.015))
	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			this.callWithF5WithGateway()
			this.callWithoutF5WithoutGateway()
			this.callWithoutF5WithGateway()
		}
	}
}

func (this *MonitorF5) callWithF5WithGateway() {
	url := "http://api.ffan.com/pubsub/v1/pub/alive"
	this.callHttp(url, this.latencyWithF5WithGateway)
}

func (this *MonitorF5) callWithoutF5WithoutGateway() {
	url := "http://pub.sit.ffan.com:9191/alive"
	this.callHttp(url, this.latencyWithoutF5WithoutGateway)
}

func (this *MonitorF5) callWithoutF5WithGateway() {
	url := "http://10.209.36.67/pubsub/v1/pub/alive"
	host := "api.ffan.com"

	buf := mpool.BytesBufferGet()
	defer mpool.BytesBufferPut(buf)

	var t time.Time
	for i := 0; i < HttpLoop; i++ {
		t = time.Now()

		buf.Reset()
		req, err := http.NewRequest("GET", url, buf)
		if err != nil {
			log.Error("%s %v", url, err)
			continue
		}

		req.Host = host
		resp, err := this.httpConn.Do(req)
		if err != nil {
			log.Error("%s %v", url, err)
			continue
		}

		resp.Body.Close()
		this.latencyWithoutF5WithGateway.Update(time.Since(t).Nanoseconds() / MsInNano)
	}

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

		resp.Body.Close()

		h.Update(time.Since(t).Nanoseconds() / MsInNano)
	}
}
