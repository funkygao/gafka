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

type PubGateway struct {
	listener net.Listener
	server   *http.Server
	router   *mux.Router

	shutdownCh chan struct{}

	metacache MetaStore
}

func NewGateway() *PubGateway {
	this := &PubGateway{
		router:     mux.NewRouter(),
		shutdownCh: make(chan struct{}),
	}
	this.server = &http.Server{
		Addr:    ":9090", // TODO
		Handler: this.router,
	}
	return this
}

func (this *PubGateway) Start() (err error) {
	this.listener, err = net.Listen("tcp", this.server.Addr)
	if err != nil {
		return
	}

	go this.server.Serve(this.listener)
	return nil
}

func (this *PubGateway) BuildRouting() {
	this.router.HandleFunc("/{ver}/topics/{topic}", this.postTopic).Methods("POST")
	this.router.HandleFunc("/ver", this.showVersion)
}

func (this *PubGateway) showVersion(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(fmt.Sprintf("%s-%s", gafka.Version, gafka.BuildId)))
}

func (this *PubGateway) postTopic(w http.ResponseWriter, req *http.Request) {
}

func (this *PubGateway) ServeForever() {
	select {
	case <-this.shutdownCh:
		break
	}
}

func (this *PubGateway) Stop() {
	if this.listener != nil {
		this.listener.Close()
		this.listener = nil
		this.server = nil
		this.router = nil

		close(this.shutdownCh)
	}
}
