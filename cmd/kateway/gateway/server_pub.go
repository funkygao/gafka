// +build !fasthttp

package gateway

import (
	"net"
	"time"

	"github.com/funkygao/golib/ratelimiter"
	log "github.com/funkygao/log4go"
)

type pubServer struct {
	*webServer

	pubMetrics  *pubMetrics
	throttlePub *ratelimiter.LeakyBuckets
}

func newPubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *pubServer {
	this := &pubServer{
		webServer:   newWebServer("pub", httpAddr, httpsAddr, maxClients, gw),
		throttlePub: ratelimiter.NewLeakyBuckets(Options.PubQpsLimit, time.Minute),
	}
	this.pubMetrics = NewPubMetrics(this.gw)
	this.onConnNewFunc = this.onConnNew
	this.onConnCloseFunc = this.onConnClose

	this.webServer.onStop = func() {
		this.pubMetrics.Flush()
		log.Trace("pub metrics flushed")
	}

	return this
}

func (this *pubServer) Start() {
	this.pubMetrics.Load()
	this.webServer.Start()
}

func (this *pubServer) onConnNew(c net.Conn) {
	if this.gw != nil && !Options.DisableMetrics {
		this.gw.svrMetrics.ConcurrentPub.Inc(1)
	}
}

func (this *pubServer) onConnClose(c net.Conn) {
	if this.gw != nil && !Options.DisableMetrics {
		this.gw.svrMetrics.ConcurrentPub.Dec(1)
	}

	if Options.EnableClientStats {
		this.gw.clientStates.UnregisterPubClient(c.RemoteAddr().String())
	}
}
