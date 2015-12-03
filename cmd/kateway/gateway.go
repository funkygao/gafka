package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/cmd/kateway/store/dumb"
	"github.com/funkygao/gafka/cmd/kateway/store/kafka"
	"github.com/funkygao/golib/breaker"
	"github.com/funkygao/golib/ratelimiter"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
)

// Gateway is a distributed kafka Pub/Sub HTTP endpoint.
type Gateway struct {
	id        string
	hostname  string
	startedAt time.Time

	// openssl genrsa -out key.pem 2048
	// openssl req -new -x509 -key key.pem -out cert.pem -days 3650
	certFile string
	keyFile  string

	meta meta.MetaStore

	pubServer *pubServer
	pubStore  store.PubStore

	subServer *subServer
	subStore  store.SubStore

	guard *guard

	routes []route

	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	wg           sync.WaitGroup

	leakyBucket *ratelimiter.LeakyBucket // TODO
	breaker     *breaker.Consecutive
	pubMetrics  *pubMetrics
	subMetrics  *subMetrics
}

func NewGateway(id string, metaRefreshInterval time.Duration) *Gateway {
	this := &Gateway{
		id:          id,
		shutdownCh:  make(chan struct{}),
		routes:      make([]route, 0),
		leakyBucket: ratelimiter.NewLeakyBucket(1000*60, time.Minute),
		certFile:    options.certFile,
		keyFile:     options.keyFile,
	}

	this.meta = meta.NewZkMetaStore(options.zone, options.cluster, metaRefreshInterval)
	this.guard = newGuard(this)
	this.breaker = &breaker.Consecutive{
		FailureAllowance: 10,
		RetryTimeout:     time.Second * 10,
	}
	this.hostname, _ = os.Hostname()

	if options.pubHttpAddr != "" || options.pubHttpsAddr != "" {
		this.pubServer = newPubServer(options.pubHttpAddr, options.pubHttpsAddr,
			options.maxClients, this)
		this.pubMetrics = newPubMetrics(this)

		switch options.store {
		case "kafka":
			this.pubStore = kafka.NewPubStore(this.meta, this.hostname,
				&this.wg, this.shutdownCh, options.debug)
		case "dumb":
			this.pubStore = dumb.NewPubStore(this.meta, this.hostname,
				&this.wg, this.shutdownCh, options.debug)
		}

	}
	if options.subHttpAddr != "" || options.subHttpsAddr != "" {
		this.subServer = newSubServer(options.subHttpAddr, options.subHttpsAddr,
			options.maxClients, this)
		this.subMetrics = newSubMetrics(this)

		switch options.store {
		case "kafka":
			this.subStore = kafka.NewSubStore(this.meta, this.hostname,
				&this.wg, this.shutdownCh, this.subServer.closedConnCh, options.debug)
		case "dumb":
			this.subStore = dumb.NewSubStore(this.meta, this.hostname,
				&this.wg, this.shutdownCh, this.subServer.closedConnCh, options.debug)
		}

	}

	return this
}

func (this *Gateway) Start() (err error) {
	signal.RegisterSignalHandler(syscall.SIGINT, func(sig os.Signal) {
		this.Stop()
	})
	signal.RegisterSignalHandler(syscall.SIGUSR2, func(sig os.Signal) {
		this.Stop()
	})

	this.startedAt = time.Now()

	this.meta.Start()
	log.Trace("meta store started")

	go this.guard.Start()
	log.Trace("guard started")

	this.buildRouting()

	if this.pubServer != nil {
		go this.pubStore.Start()

		this.pubServer.Start()
	}
	if this.subServer != nil {
		go this.subStore.Start()

		this.subServer.Start()
	}

	this.registerInZk()

	log.Info("gateway[%s:%s] ready", this.hostname, this.id)

	return nil
}

func (this *Gateway) Stop() {
	this.shutdownOnce.Do(func() {
		close(this.shutdownCh)
	})

}

func (this *Gateway) ServeForever() {
	select {
	case <-this.shutdownCh:

		// wait for all components shutdown
		log.Trace("waiting for all components shutdown...")
		this.wg.Wait()
		log.Trace("all components shutdown complete")

		this.meta.Stop()

		log.Info("gateway[%s:%s] shutdown complete", this.hostname, this.id)
	}

}

func (this *Gateway) setupHttpsServer(server *http.Server, certFile, keyFile string) (net.Listener, error) {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return nil, err
	}

	config := &tls.Config{}
	config.NextProtos = []string{"http/1.1"}
	config.Certificates = make([]tls.Certificate, 1)
	config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	tlsListener := tls.NewListener(listener, config)
	return tlsListener, nil
}
