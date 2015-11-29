package main

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

type subServer struct {
	maxClients int
	wg         *sync.WaitGroup

	listener net.Listener
	server   *http.Server
	router   *mux.Router

	idleConnsWg   sync.WaitGroup      // wait for all inflight http connections done
	idleConns     map[string]net.Conn // in keep-alive state http connections
	closedConnCh  chan string         // channel of remote addr
	idleConnsLock sync.Mutex

	exitCh <-chan struct{}
}

func newSubServer(port int, maxClients int, wg *sync.WaitGroup, exitCh <-chan struct{}) *subServer {
	this := &subServer{
		exitCh:       exitCh,
		wg:           wg,
		maxClients:   maxClients,
		router:       mux.NewRouter(),
		closedConnCh: make(chan string, 1<<10),
		idleConns:    make(map[string]net.Conn, 10000),
	}
	this.server = &http.Server{
		Addr:           fmt.Sprintf(":%d", port),
		Handler:        this.router,
		ReadTimeout:    0,       // FIXME
		WriteTimeout:   0,       // FIXME
		MaxHeaderBytes: 4 << 10, // should be enough
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
			case <-this.exitCh:
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

	return this
}

func (this *subServer) Start() {
	var err error
	this.listener, err = net.Listen("tcp", this.server.Addr)
	if err != nil {
		return
	}

	this.listener = LimitListener(this.listener, this.maxClients)
	go this.server.Serve(this.listener)
	go this.waitExit()

	this.wg.Add(1)
	log.Info("sub http server ready on :%s", this.server.Addr)
}

func (this *subServer) Router() *mux.Router {
	return this.router
}

func (this *subServer) waitExit() {
	for {
		select {
		case <-this.exitCh:
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

			// wait for all connected http client close
			this.idleConnsWg.Wait()

			this.listener = nil
			this.server = nil
			this.router = nil

			this.wg.Done()

		}
	}
}
