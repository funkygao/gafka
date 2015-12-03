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
		if this.httpServer != nil {
			// HTTP response will have "Connection: close"
			this.httpServer.SetKeepAlivesEnabled(false)

			// avoid new connections
			if err := this.httpListener.Close(); err != nil {
				log.Error("pub http listener close: %v", err)
			}

			this.gw.wg.Done()
			log.Trace("%s http server stopped", this.name)
		}

		if this.httpsServer != nil {
			this.gw.wg.Done()
			log.Trace("%s https server stopped", this.name)
		}

		this.httpListener = nil
		this.tlsListener = nil
		this.httpServer = nil
		this.httpsServer = nil
		this.router = nil
	}

}
