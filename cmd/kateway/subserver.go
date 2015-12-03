package main

import (
	"net"
	"net/http"
	"sync"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

type subServer struct {
	maxClients int
	gw         *Gateway

	listener net.Listener
	server   *http.Server

	tlsListener net.Listener
	httpsServer *http.Server

	router *mux.Router

	idleConnsWg   sync.WaitGroup      // wait for all inflight http connections done
	idleConns     map[string]net.Conn // in keep-alive state http connections
	closedConnCh  chan string         // channel of remote addr
	idleConnsLock sync.Mutex

	once sync.Once
}

func newSubServer(httpAddr, httpsAddr string, maxClients int, gw *Gateway) *subServer {
	this := &subServer{
		gw:           gw,
		maxClients:   maxClients,
		router:       mux.NewRouter(),
		closedConnCh: make(chan string, 1<<10),
		idleConns:    make(map[string]net.Conn, 10000),
	}

	if httpAddr != "" {
		this.server = &http.Server{
			Addr:           httpAddr,
			Handler:        this.router,
			ReadTimeout:    time.Minute, // FIXME
			WriteTimeout:   time.Minute, // FIXME
			MaxHeaderBytes: 4 << 10,     // should be enough
		}

		// register the http conn state machine hook
		// FIXME should distinguish pub from sub client
		this.server.ConnState = func(c net.Conn, cs http.ConnState) {
			switch cs {
			case http.StateNew:
				this.idleConnsWg.Add(1)

			case http.StateActive:
				this.idleConnsLock.Lock()
				delete(this.idleConns, c.RemoteAddr().String())
				this.idleConnsLock.Unlock()

			case http.StateIdle:
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
				log.Debug("http client[%s] closed", c.RemoteAddr())
				this.closedConnCh <- c.RemoteAddr().String()
				this.idleConnsWg.Done()
			}
		}
	}

	if httpsAddr != "" {
		this.httpsServer = &http.Server{
			Addr:           httpAddr,
			Handler:        this.router,
			ReadTimeout:    0,       // FIXME
			WriteTimeout:   0,       // FIXME
			MaxHeaderBytes: 4 << 10, // should be enough
		}
	}

	return this
}

func (this *subServer) Start() {
	var err error
	if this.server != nil {
		this.listener, err = net.Listen("tcp", this.server.Addr)
		if err != nil {
			panic(err)
		}

		this.listener = LimitListener(this.listener, this.maxClients)
		go this.server.Serve(this.listener)

		this.gw.wg.Add(1)
		this.once.Do(func() {
			go this.waitExit()
		})

		log.Info("sub http server ready on %s", this.server.Addr)
	}

	if this.httpsServer != nil {
		this.tlsListener, err = this.gw.setupHttpsServer(this.httpsServer,
			this.gw.certFile, this.gw.keyFile)
		if err != nil {
			panic(err)
		}

		this.tlsListener = LimitListener(this.tlsListener, this.maxClients)
		go this.httpsServer.Serve(this.tlsListener)

		this.gw.wg.Add(1)
		this.once.Do(func() {
			go this.waitExit()
		})

		log.Info("sub https server ready on %s", this.server.Addr)
	}

}

func (this *subServer) Router() *mux.Router {
	return this.router
}

func (this *subServer) waitExit() {
	select {
	case <-this.gw.shutdownCh:
		// TODO https server

		// HTTP response will have "Connection: close"
		this.server.SetKeepAlivesEnabled(false)

		// avoid new connections
		if err := this.listener.Close(); err != nil {
			log.Error("listener close: %v", err)
		}

		this.idleConnsLock.Lock()
		t := time.Now().Add(time.Millisecond * 100)
		for _, c := range this.idleConns {
			c.SetReadDeadline(t)
		}
		this.idleConnsLock.Unlock()

		log.Trace("waiting for all connected http client close")
		this.idleConnsWg.Wait()

		if this.server != nil {
			this.gw.wg.Done()
			log.Trace("subserver http stopped")
		}
		if this.httpsServer != nil {
			this.gw.wg.Done()
			log.Trace("subserver https stopped")
		}

		this.listener = nil
		this.server = nil
		this.router = nil
	}

}
