package main

import (
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

func (this *manServer) waitExit(exit <-chan struct{}) {
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
