// +build !fasthttp

package main

import (
	"net"
	"net/http"
)

type pubServer struct {
	*webServer
}

func newPubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *pubServer {
	this := &pubServer{
		webServer: newWebServer("pub", httpAddr, httpsAddr, maxClients, gw),
	}

	if this.httpServer != nil {
		this.httpServer.ConnState = func(c net.Conn, cs http.ConnState) {
			switch cs {
			case http.StateNew:
				if this.gw != nil && !options.disableMetrics {
					this.gw.svrMetrics.ConcurrentPub.Inc(1)
				}

			case http.StateActive, http.StateIdle:
				// do nothing

			case http.StateClosed, http.StateHijacked:
				if this.gw != nil && !options.disableMetrics {
					this.gw.svrMetrics.ConcurrentPub.Dec(1)
				}

				// deregister the online producer
				this.gw.produersLock.Lock()
				delete(this.gw.producers, c.RemoteAddr().String())
				this.gw.produersLock.Unlock()
			}
		}
	}

	return this
}
