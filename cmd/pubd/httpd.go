package main

import (
	"net"
	"net/http"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/gorilla/mux"
)

type PubGateway struct {
	listener net.Listener
	server   *http.Server
	router   *mux.Router
}

func (this *PubGateway) ServeForever() (err error) {
	this.router = mux.NewRouter()
	this.server = &http.Server{
		Addr:    ":9090",
		Handler: this.router,
	}

	this.listener, err = net.Listen("tcp", this.server.Addr)
	if err != nil {
		return
	}

	go this.server.Serve(this.listener)
	return nil
}

func (this *PubGateway) Setup() {
	this.router.HandleFunc("/{ver}/topics/{topic}", this.postTopic).Methods("POST")
}

func (this *PubGateway) postTopic(req http.ResponseWriter, rep *http.Request) {
}

func (this *PubGateway) Stop() {
	if this.listener != nil {
		this.listener.Close()
	}
}
