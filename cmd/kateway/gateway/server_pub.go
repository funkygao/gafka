// +build !fasthttp

package gateway

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
