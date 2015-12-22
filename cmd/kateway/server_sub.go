package main

import (
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	log "github.com/funkygao/log4go"
)

type subServer struct {
	*webServer

	idleConnsWg   sync.WaitGroup      // wait for all inflight http connections done
	idleConns     map[string]net.Conn // in keep-alive state http connections
	closedConnCh  chan string         // channel of remote addr
	idleConnsLock sync.Mutex
}

func newSubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *subServer {
	this := &subServer{
		webServer:    newWebServer("sub", httpAddr, httpsAddr, maxClients, gw),
		closedConnCh: make(chan string, 1<<10),
		idleConns:    make(map[string]net.Conn, 10000), // TODO
	}
	this.waitExitFunc = this.waitExit

	if this.httpsServer != nil {
		// TODO
	}

	if this.httpServer != nil {
		this.httpServer.ConnState = func(c net.Conn, cs http.ConnState) {
			switch cs {
			case http.StateNew:
				// Connections begin at StateNew and then
				// transition to either StateActive or StateClosed
				this.idleConnsWg.Add(1)

			case http.StateActive:
				// StateActive fires before the request has entered a handler
				// and doesn't fire again until the request has been
				// handled.
				// After the request is handled, the state
				// transitions to StateClosed, StateHijacked, or StateIdle.
				this.idleConnsLock.Lock()
				delete(this.idleConns, c.RemoteAddr().String())
				this.idleConnsLock.Unlock()

			case http.StateIdle:
				// StateIdle represents a connection that has finished
				// handling a request and is in the keep-alive state, waiting
				// for a new request. Connections transition from StateIdle
				// to either StateActive or StateClosed.
				select {
				case <-this.gw.shutdownCh:
					// actively close the client safely because IO is all done
					c.Close()

				default:
					this.idleConnsLock.Lock()
					this.idleConns[c.RemoteAddr().String()] = c
					this.idleConnsLock.Unlock()
				}

			case http.StateClosed:
				log.Debug("remote http client[%s] closed", c.RemoteAddr())
				this.closedConnCh <- c.RemoteAddr().String()
				this.idleConnsWg.Done()
			}
		}
	}

	return this
}

func (this *subServer) waitExit(exit <-chan struct{}) {
	select {
	case <-exit:
		if this.httpServer != nil {
			// HTTP response will have "Connection: close"
			this.httpServer.SetKeepAlivesEnabled(false)

			// avoid new connections
			if err := this.httpListener.Close(); err != nil {
				log.Error(err.Error())
			}

			this.idleConnsLock.Lock()
			t := time.Now().Add(time.Millisecond * 100)
			for _, c := range this.idleConns {
				c.SetReadDeadline(t)
			}
			this.idleConnsLock.Unlock()

			log.Trace("%s waiting for all connected http client close", this.name)
			this.idleConnsWg.Wait()

			this.gw.wg.Done()
			log.Trace("%s http server stopped", this.name)
		}

		if this.httpsServer != nil {
			// TODO
			this.gw.wg.Done()
			log.Trace("%s https server stopped", this.name)
		}

	}

}
