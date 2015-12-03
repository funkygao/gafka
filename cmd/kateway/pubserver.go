package main

import (
	log "github.com/funkygao/log4go"
)

type pubServer struct {
	*webServer
}

func newPubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *pubServer {
	this := &pubServer{
		webServer: newWebServer("pub", httpAddr, httpsAddr, maxClients, gw),
	}
	this.waitExitFunc = this.waitExit

	return this
}

func (this *pubServer) waitExit(exit <-chan struct{}) {
	select {
	case <-exit:

		// HTTP response will have "Connection: close"
		this.server.SetKeepAlivesEnabled(false)

		// avoid new connections
		if err := this.listener.Close(); err != nil {
			log.Error("listener close: %v", err)
		}

		if this.server != nil {
			this.gw.wg.Done()
			log.Trace("%s http server stopped", this.name)
		}
		if this.httpsServer != nil {
			this.gw.wg.Done()
			log.Trace("%s https server stopped", this.name)
		}

		this.listener = nil
		this.server = nil
		this.router = nil
	}

}
