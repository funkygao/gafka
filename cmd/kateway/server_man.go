package main

import (
	"net"
	"net/http"

	log "github.com/funkygao/log4go"
)

// management server
type manServer struct {
	*webServer
}

func newManServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *manServer {
	this := &manServer{
		webServer: newWebServer("man", httpAddr, httpsAddr, maxClients, gw),
	}
	this.waitExitFunc = this.waitExit

	return this
}

func (this *manServer) waitExit(server *http.Server, listener net.Listener, exit <-chan struct{}) {
	<-exit

	// HTTP response will have "Connection: close"
	server.SetKeepAlivesEnabled(false)

	// avoid new connections
	if err := listener.Close(); err != nil {
		log.Error(err.Error())
	}

	this.gw.wg.Done()
	log.Trace("%s server stopped on %s", this.name, server.Addr)
}
