package main

import (
	"fmt"
	"net"
	"net/http"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/funkygao/gafka"
	"github.com/gorilla/mux"
)

type Gateway struct {
	mode string

	listener net.Listener
	server   *http.Server
	router   *mux.Router

	shutdownCh chan struct{}

	metacache MetaStore
}

func NewGateway(mode string) *Gateway {
	this := &Gateway{
		mode:       mode,
		router:     mux.NewRouter(),
		shutdownCh: make(chan struct{}),
	}
	this.server = &http.Server{
		Addr:    ":9090", // TODO
		Handler: this.router,
	}
	return this
}

func (this *Gateway) Start() (err error) {
	this.buildRouting()

	this.listener, err = net.Listen("tcp", this.server.Addr)
	if err != nil {
		return
	}

	//go this.metacache.Start()
	go this.server.Serve(this.listener)

	return nil
}

func (this *Gateway) buildRouting() {
	this.router.HandleFunc("/ver", this.showVersion)

	if this.mode == "pub" || this.mode == "pubsub" {
		this.router.HandleFunc("/{ver}/topics/{topic}", this.pubHandler).Methods("POST")
	}
	if this.mode == "sub" || this.mode == "pubsub" {
		this.router.HandleFunc("/topics/{topic}", this.subHandler).Methods("GET")
	}
}

func (this *Gateway) showVersion(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(fmt.Sprintf("%s-%s", gafka.Version, gafka.BuildId)))
}

func (this *Gateway) writeAuthFailure(w http.ResponseWriter) {
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte("invalid pubkey"))
}

func (this *Gateway) ServeForever() {
	select {
	case <-this.shutdownCh:
		break
	}
}

func (this *Gateway) Stop() {
	if this.listener != nil {
		this.listener.Close()
		this.listener = nil
		this.server = nil
		this.router = nil

		this.metacache.Stop()

		close(this.shutdownCh)
	}
}
