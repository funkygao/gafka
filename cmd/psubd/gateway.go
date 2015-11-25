package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"syscall"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/funkygao/gafka"
	"github.com/funkygao/golib/breaker"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
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

	kpool   *kpool
	breaker *breaker.Consecutive
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
		Addr:    fmt.Sprintf(":%d", options.port),
		Handler: this.router,
	}
	this.breaker = &breaker.Consecutive{
		FailureAllowance: 10,
		RetryTimeout:     time.Second * 10,
	}
	switch mode {
	case "pub", "pubsub":
		this.metrics = newPubMetrics()

	case "sub":

	}
	return this
}

func (this *Gateway) Start() (err error) {
	signal.RegisterSignalHandler(syscall.SIGINT, func(sig os.Signal) {
		this.Stop()
	})

	this.buildRouting()

	this.metaStore.Start()
	this.kpool = newKpool(this.metaStore.BrokerList())

	this.listener, err = net.Listen("tcp", this.server.Addr)
	if err != nil {
		return
	}

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
		this.router.HandleFunc("/{ver}/topics/{topic}", this.subHandler).Methods("GET")
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

// TODO
func (this *Gateway) authenticate(req *http.Request) (ok bool) {
	switch this.mode {
	case "pub":
		pubkeyParam := req.Header["Pubkey"]
		if len(pubkeyParam) > 0 && !this.metaStore.AuthPub(pubkeyParam[0]) {
			log.Error("client:%s pubkey: %s", req.RemoteAddr, pubkeyParam[0])
			return false
		}

		return true

	default:
		return true
	}

}

func (this *Gateway) writeAuthFailure(w http.ResponseWriter) {
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte("invalid pubkey"))
}

func (this *Gateway) writeBreakerOpen(w http.ResponseWriter) {
	w.WriteHeader(http.StatusBadGateway)
	w.Write([]byte("circuit broken"))
}

func (this *Gateway) writeBadRequest(w http.ResponseWriter) {
	w.WriteHeader(http.StatusBadRequest)
}

func (this *Gateway) ServeForever() {
	meteRefreshTicker := time.NewTicker(this.metaRefreshInterval)
	defer meteRefreshTicker.Stop()
	ever := true
	for ever {
		select {
		case <-this.shutdownCh:
			log.Info("gateway terminated")
			ever = false
			break

		case <-meteRefreshTicker.C:
			this.metaStore.Refresh()
			this.kpool.RefreshBrokerList(this.metaStore.BrokerList())
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
