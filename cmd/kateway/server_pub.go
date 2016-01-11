// +build !fasthttp

package main

import (
	"net"
	"net/http"

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

	if this.httpServer != nil {
		this.httpServer.ConnState = func(c net.Conn, cs http.ConnState) {
			switch cs {
			case http.StateNew, http.StateActive, http.StateIdle:
				// do nothing

			case http.StateClosed:
				this.gw.produersLock.Lock()
				delete(this.gw.producers, c.RemoteAddr().String())
				this.gw.produersLock.Unlock()
			}
		}
	}

	return this
}

func (this *pubServer) waitExit(server *http.Server, listener net.Listener, exit <-chan struct{}) {
	<-exit

	// HTTP response will have "Connection: close"
	server.SetKeepAlivesEnabled(false)

	// avoid new connections
	if err := listener.Close(); err != nil {
		log.Error(err.Error())
	}

	this.gw.wg.Done()
}
