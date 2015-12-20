// +build fasthttp

package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/buaazp/fasthttprouter"
	log "github.com/funkygao/log4go"
	"github.com/valyala/fasthttp"
)

type pubServer struct {
	name       string
	maxClients int
	gw         *Gateway

	httpListener net.Listener
	httpServer   *fasthttp.Server

	tlsListener net.Listener
	httpsServer *http.Server

	router *fasthttprouter.Router

	waitExitFunc waitExitFunc

	once sync.Once
}

func newPubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *pubServer {
	this := &pubServer{
		webServer: newWebServer("fastpub", httpAddr, httpsAddr, maxClients, gw),
	}
	this.waitExitFunc = this.waitExit

	return this
}

func (this *pubServer) Start() {

}

func (this *pubServer) Router() *fasthttprouter.Router {
	return this.router
}

func (this *pubServer) waitExit(exit <-chan struct{}) {
	select {
	case <-exit:
		if this.httpServer != nil {
			// HTTP response will have "Connection: close"
			this.httpServer.SetKeepAlivesEnabled(false)

			// avoid new connections
			if err := this.httpListener.Close(); err != nil {
				log.Error(err.Error())
			}

			this.gw.wg.Done()
			log.Trace("%s http server stopped", this.name)
		}

		if this.httpsServer != nil {
			this.gw.wg.Done()
			log.Trace("%s https server stopped", this.name)
		}

	}

}
