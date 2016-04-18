package main

import (
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/funkygao/gafka/mpool"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

const (
	MsInNano = 1000 * 1000
	HttpLoop = 100
)

// MonitorF5 monitors latency of F5 load balancer vibration.
type MonitorF5 struct {
	stop chan struct{}
	tick time.Duration
	wg   *sync.WaitGroup

	latencyWithF5WithGateway       metrics.Histogram
	latencyWithoutF5WithoutGateway metrics.Histogram
	latencyWithoutF5WithGateway    metrics.Histogram
	latencyF5DirectBackend         metrics.Histogram
	latencyRealUser                metrics.Histogram

	errors metrics.Meter // all kind of errors in 1 meter

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
	this.latencyF5DirectBackend = metrics.NewRegisteredHistogram("latency.api.f5.backend", nil, metrics.NewExpDecaySample(1028, 0.015))
	this.latencyRealUser = metrics.NewRegisteredHistogram("latency.api.f5.realuser", nil, metrics.NewExpDecaySample(1028, 0.015))
	this.errors = metrics.NewRegisteredMeter("latency.api.f5.err", metrics.DefaultRegistry)

	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			this.callWithF5WithGateway()
			this.callWithoutF5WithoutGateway()
			this.callWithoutF5WithGateway()
			this.callWithF5DirectToBackend()
			this.callLikeRealUser()
		}
	}
}

// client -> F5(intra) -> gateway -> F5(intra) -> nginx -> backend
func (this *MonitorF5) callLikeRealUser() {
	url := "http://api.ffan.com/health/v1/callChain"
	// http://pubf5gwng.intra.ffan.com/alive
	this.callHttp(url, this.latencyRealUser)
}

// client -> F5(intra) -> [nginx] -> gateway -> backend
func (this *MonitorF5) callWithF5WithGateway() {
	url := "http://api.ffan.com/pubsub/v1/pub/alive"
	// http://pub.intra.ffan.com/alive
	this.callHttp(url, this.latencyWithF5WithGateway)
}

// client -> F5(intra) -> backend
func (this *MonitorF5) callWithF5DirectToBackend() {
	url := "http://10.208.224.47/alive"
	// http://10.213.57.147:9191
	this.callHttp(url, this.latencyF5DirectBackend)
}

// client -> backend
func (this *MonitorF5) callWithoutF5WithoutGateway() {
	url := "http://pub.sit.ffan.com:9191/alive"
	this.callHttp(url, this.latencyWithoutF5WithoutGateway)
}

// client -> nginx(intra) -> gateway -> backend
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
			this.errors.Mark(1)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Error("%s %s", url, resp.Status)
			this.errors.Mark(1)
			continue
		}

		var b []byte
		b, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Error("%s %s", url, resp.Status)
			this.errors.Mark(1)
		} else if string(b) != `{"ok": 1}` {
			log.Error("%s response: %s", url, string(b))
			this.errors.Mark(1)
		}

		resp.Body.Close()

		h.Update(time.Since(t).Nanoseconds() / MsInNano)
	}
}
