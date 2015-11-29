package main

import (
	"fmt"
	"net"
	"net/http"
	"sync"

	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

type pubServer struct {
	maxClients int
	wg         *sync.WaitGroup

	listener net.Listener
	server   *http.Server
	router   *mux.Router

	exitCh <-chan struct{}
}

func newPubServer(port int, maxClients int, wg *sync.WaitGroup, exitCh <-chan struct{}) *pubServer {
	this := &pubServer{
		exitCh:     exitCh,
		router:     mux.NewRouter(),
		wg:         wg,
		maxClients: maxClients,
	}
	this.server = &http.Server{
		Addr:           fmt.Sprintf(":%d", port),
		Handler:        this.router,
		ReadTimeout:    0,       // FIXME
		WriteTimeout:   0,       // FIXME
		MaxHeaderBytes: 4 << 10, // should be enough
	}

	return this
}

func (this *pubServer) Start() {
	var err error
	this.listener, err = net.Listen("tcp", this.server.Addr)
	if err != nil {
		return
	}

	this.listener = LimitListener(this.listener, this.maxClients)
	go this.server.Serve(this.listener)
	go this.waitExit()

	this.wg.Add(1)
	log.Info("pub http server ready on :%s", this.server.Addr)
}

func (this *pubServer) Router() *mux.Router {
	return this.router
}

func (this *pubServer) waitExit() {
	for {
		select {
		case <-this.exitCh:
			// HTTP response will have "Connection: close"
			this.server.SetKeepAlivesEnabled(false)

			// avoid new connections
			if err := this.listener.Close(); err != nil {
				log.Error("listener close: %v", err)
			}

			this.listener = nil
			this.server = nil
			this.router = nil

			this.wg.Done()

		}
	}
}
