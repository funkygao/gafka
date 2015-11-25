package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

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

	metrics             *pubMetrics
	metaStore           MetaStore
	metaRefreshInterval time.Duration
}

func NewGateway(mode string, metaRefreshInterval time.Duration) *Gateway {
	this := &Gateway{
		mode:                mode,
		router:              mux.NewRouter(),
		shutdownCh:          make(chan struct{}),
		metaStore:           newZkMetaStore(options.zone, options.cluster),
		metaRefreshInterval: metaRefreshInterval,
	}
	this.server = &http.Server{
		Addr:    ":9090", // TODO
		Handler: this.router,
	}
	switch mode {
	case "pub":
		this.metrics = newPubMetrics()
	}
	return this
}

func (this *Gateway) Start() (err error) {
	this.buildRouting()

	this.listener, err = net.Listen("tcp", this.server.Addr)
	if err != nil {
		return
	}

	this.metaStore.Start()
	go this.server.Serve(this.listener)

	return nil
}

func (this *Gateway) buildRouting() {
	this.router.HandleFunc("/ver", this.showVersion)
	this.router.HandleFunc("/clusters", this.showClusters)

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

func (this *Gateway) showClusters(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	b, _ := json.Marshal(this.metaStore.Clusters())
	w.Write(b)
}

func (this *Gateway) writeAuthFailure(w http.ResponseWriter) {
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte("invalid pubkey"))
}

func (this *Gateway) ServeForever() {
	meteRefreshTicker := time.NewTicker(this.metaRefreshInterval)
	for {
		select {
		case <-this.shutdownCh:
			meteRefreshTicker.Stop()
			break

		case <-meteRefreshTicker.C:
			this.metaStore.Refresh()
		}
	}

}

func (this *Gateway) Stop() {
	if this.listener != nil {
		this.listener.Close()
		this.listener = nil
		this.server = nil
		this.router = nil

		this.metaStore.Stop()

		close(this.shutdownCh)
	}
}
