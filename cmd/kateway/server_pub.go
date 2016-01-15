// +build !fasthttp

package main

import (
	"net"
)

type pubServer struct {
	*webServer
}

func newPubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *pubServer {
	this := &pubServer{
		webServer: newWebServer("pub", httpAddr, httpsAddr, maxClients, gw),
	}
	this.onConnNewFunc = this.onConnNew
	this.onConnCloseFunc = this.onConnClose

	return this
}

func (this *pubServer) onConnNew(c net.Conn) {
	if this.gw != nil && !options.DisableMetrics {
		this.gw.svrMetrics.ConcurrentPub.Inc(1)
	}
}

func (this *pubServer) onConnClose(c net.Conn) {
	if this.gw != nil && !options.DisableMetrics {
		this.gw.svrMetrics.ConcurrentPub.Dec(1)
	}

	// deregister the online producer
	this.gw.produersLock.Lock()
	delete(this.gw.producers, c.RemoteAddr().String())
	this.gw.produersLock.Unlock()
}
