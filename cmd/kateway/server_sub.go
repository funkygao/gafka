package main

import (
	"net"
	"net/http"
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

	// websocket heartbeat configuration
	wsReadLimit int64
	wsPongWait  time.Duration
}

func newSubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *subServer {
	this := &subServer{
		webServer:    newWebServer("sub", httpAddr, httpsAddr, maxClients, gw),
		closedConnCh: make(chan string, 1<<10),
		idleConns:    make(map[string]net.Conn, 10000), // TODO
		wsReadLimit:  8 << 10,
		wsPongWait:   time.Minute,
	}
	this.waitExitFunc = this.waitExit
	this.connStateFunc = this.connStateHandler

	if this.httpsServer != nil {
		this.httpsServer.ConnState = this.connStateHandler
	}

	if this.httpServer != nil {
		this.httpServer.ConnState = this.connStateHandler
	}

	return this
}

func (this *subServer) connStateHandler(c net.Conn, cs http.ConnState) {
	switch cs {
	case http.StateNew:
		// Connections begin at StateNew and then
		// transition to either StateActive or StateClosed
		this.idleConnsWg.Add(1)

		if this.gw != nil && !options.DisableMetrics {
			this.gw.svrMetrics.ConcurrentSub.Inc(1)
		}

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

	case http.StateHijacked:
		// websocket steals the socket
		if this.gw != nil && !options.DisableMetrics {
			this.gw.svrMetrics.ConcurrentSub.Dec(1)

			this.gw.svrMetrics.ConcurrentSubWs.Inc(1)
		}

	case http.StateClosed:
		if this.gw != nil && !options.DisableMetrics {
			this.gw.svrMetrics.ConcurrentSub.Dec(1)
		}

		this.closedConnCh <- c.RemoteAddr().String()
		this.idleConnsWg.Done()
	}
}

func (this *subServer) waitExit(server *http.Server, listener net.Listener, exit <-chan struct{}) {
	<-exit

	// HTTP response will have "Connection: close"
	server.SetKeepAlivesEnabled(false)

	// avoid new connections
	if err := listener.Close(); err != nil {
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
}
