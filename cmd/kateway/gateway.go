package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/funkygao/golib/breaker"
	"github.com/funkygao/golib/ratelimiter"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
	"github.com/gorilla/mux"
)

// Gateway is a kafka Pub/Sub HTTP endpoint.
type Gateway struct {
	mode     string
	hostname string

	listener net.Listener
	server   *http.Server
	router   *mux.Router
	routes   []route

	shutdownCh chan struct{}
	wg         sync.WaitGroup

	leakyBucket *ratelimiter.LeakyBucket // TODO
	breaker     *breaker.Consecutive
	pubMetrics  *pubMetrics
	subMetrics  *subMetrics

	metaStore           MetaStore
	metaRefreshInterval time.Duration

	pubPool *pubPool
	subPool *subPool
}

func NewGateway(mode string, metaRefreshInterval time.Duration) *Gateway {
	this := &Gateway{
		mode:                mode,
		router:              mux.NewRouter(),
		shutdownCh:          make(chan struct{}),
		routes:              make([]route, 0),
		metaStore:           newZkMetaStore(options.zone, options.cluster),
		leakyBucket:         ratelimiter.NewLeakyBucket(1000*60, time.Minute),
		metaRefreshInterval: metaRefreshInterval,
	}
	this.server = &http.Server{
		Addr:           fmt.Sprintf(":%d", options.port),
		Handler:        this.router,
		ReadTimeout:    options.httpReadTimeout,  // FIXME will kill the consumer
		WriteTimeout:   options.httpWriteTimeout, // FIXME will kill the producer
		MaxHeaderBytes: 4 << 10,
	}
	this.breaker = &breaker.Consecutive{
		FailureAllowance: 10,
		RetryTimeout:     time.Second * 10,
	}
	this.hostname, _ = os.Hostname()
	switch mode {
	case "pub":
		this.pubMetrics = newPubMetrics(this)

	case "sub":
		this.subMetrics = newSubMetrics(this)

	case "pubsub":
		this.pubMetrics = newPubMetrics(this)
		this.subMetrics = newSubMetrics(this)
	}
	return this
}

func (this *Gateway) Start() (err error) {
	signal.RegisterSignalHandler(syscall.SIGINT, func(sig os.Signal) {
		this.Stop()
	})

	this.metaStore.Start()
	log.Info("gateway[%s:%s] meta store started", this.hostname, this.mode)

	switch this.mode {
	case "pub":
		this.pubPool = newPubPool(this, this.metaStore.BrokerList())
		log.Info("gateway[%s:%s] kafka pub pool started", this.hostname, this.mode)

	case "sub":
		this.subPool = newSubPool(this)
		this.wg.Add(1)
		go this.subPool.Start()
		log.Info("gateway[%s:%s] kafka consumer groups pool started", this.hostname, this.mode)

	case "pubsub":
		this.pubPool = newPubPool(this, this.metaStore.BrokerList())
		log.Info("gateway[%s:%s] kafka pub pool started", this.hostname, this.mode)

		this.subPool = newSubPool(this)
		this.wg.Add(1)
		go this.subPool.Start()
		log.Info("gateway[%s:%s] kafka consumer groups pool started", this.hostname, this.mode)
	}

	this.listener, err = net.Listen("tcp", this.server.Addr)
	if err != nil {
		return
	}
	this.listener = LimitListener(this.listener, options.maxClients)

	this.buildRouting()
	go this.server.Serve(this.listener)

	log.Info("gateway[%s:%s] http ready on %s", this.hostname, this.mode, this.server.Addr)

	return nil
}

func (this *Gateway) ServeForever() {
	meteRefreshTicker := time.NewTicker(this.metaRefreshInterval)
	defer meteRefreshTicker.Stop()
	ever := true
	for ever {
		select {
		case <-this.shutdownCh:
			log.Info("gateway[%s:%s] terminated", this.hostname, this.mode)
			ever = false
			break

		case <-meteRefreshTicker.C:
			this.metaStore.Refresh()
			if this.pubPool != nil {
				this.pubPool.RefreshBrokerList(this.metaStore.BrokerList())
			}
		}
	}

}

func (this *Gateway) Stop() {
	if this.listener != nil {
		log.Info("gateway[%s:%s] shutting down", this.hostname, this.mode)

		this.listener.Close()
		this.listener = nil
		this.server = nil
		this.router = nil

		this.metaStore.Stop()

		// FIXME not able to shudown gracefully
		close(this.shutdownCh)
		this.wg.Wait()

		log.Info("gateway[%s:%s] shutdown complete", this.hostname, this.mode)
	}
}
