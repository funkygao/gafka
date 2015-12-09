package main

import (
	"os"
	"sync"
	"syscall"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/meta/zkmeta"
	"github.com/funkygao/gafka/cmd/kateway/registry"
	"github.com/funkygao/gafka/cmd/kateway/registry/consul"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/cmd/kateway/store/dumb"
	"github.com/funkygao/gafka/cmd/kateway/store/kafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/golib/breaker"
	"github.com/funkygao/golib/ratelimiter"
	"github.com/funkygao/golib/signal"
	"github.com/funkygao/golib/timewheel"
	log "github.com/funkygao/log4go"
)

// Gateway is a distributed kafka Pub/Sub HTTP endpoint.
type Gateway struct {
	id        string
	startedAt time.Time

	// openssl genrsa -out key.pem 2048
	// openssl req -new -x509 -key key.pem -out cert.pem -days 3650
	certFile string
	keyFile  string

	pubServer *pubServer
	subServer *subServer
	manServer *manServer

	guard *guard
	timer *timewheel.TimeWheel

	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	wg           sync.WaitGroup

	leakyBuckets *ratelimiter.LeakyBuckets // TODO
	breaker      *breaker.Consecutive
	pubMetrics   *pubMetrics
	subMetrics   *subMetrics
}

func NewGateway(id string, metaRefreshInterval time.Duration) *Gateway {
	this := &Gateway{
		id:           id,
		shutdownCh:   make(chan struct{}),
		leakyBuckets: ratelimiter.NewLeakyBuckets(1000*60, time.Minute),
		certFile:     options.certFile,
		keyFile:      options.keyFile,
	}

	if options.consulAddr != "" {
		var err error
		registry.Default, err = consul.NewBackend(&ctx.Consul{
			Addr:          options.consulAddr,
			ServiceName:   "kateway",
			CheckInterval: time.Second * 10,
			CheckTimeout:  time.Second * 30,
		}, ":9191") // TODO multiple service addr
		if err != nil {
			panic(err)
		}
	}

	metaConf := zkmeta.DefaultConfig(options.zone)
	metaConf.Refresh = metaRefreshInterval
	meta.Default = zkmeta.New(metaConf)
	this.guard = newGuard(this)
	this.timer = timewheel.NewTimeWheel(time.Second, 120)
	this.breaker = &breaker.Consecutive{
		FailureAllowance: 10,
		RetryTimeout:     time.Second * 10,
	}

	this.manServer = newManServer(options.manHttpAddr, options.manHttpsAddr,
		options.maxClients, this)

	if options.pubHttpAddr != "" || options.pubHttpsAddr != "" {
		this.pubServer = newPubServer(options.pubHttpAddr, options.pubHttpsAddr,
			options.maxClients, this)
		this.pubMetrics = newPubMetrics(this)

		switch options.store {
		case "kafka":
			store.DefaultPubStore = kafka.NewPubStore(&this.wg, options.debug)

		case "dumb":
			store.DefaultPubStore = dumb.NewPubStore(&this.wg, options.debug)
		}
	}

	if options.subHttpAddr != "" || options.subHttpsAddr != "" {
		this.subServer = newSubServer(options.subHttpAddr, options.subHttpsAddr,
			options.maxClients, this)
		this.subMetrics = newSubMetrics(this)

		switch options.store {
		case "kafka":
			store.DefaultSubStore = kafka.NewSubStore(&this.wg,
				this.subServer.closedConnCh, options.debug)

		case "dumb":
			store.DefaultSubStore = dumb.NewSubStore(&this.wg,
				this.subServer.closedConnCh, options.debug)

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

	meta.Default.Start()
	log.Trace("meta store started")

	this.guard.Start()
	log.Trace("guard started")

	this.buildRouting()
	this.manServer.Start()

	if this.pubServer != nil {
		store.DefaultPubStore.Start()
		log.Info("pub store[%s] started", store.DefaultPubStore.Name())

		this.pubServer.Start()
	}
	if this.subServer != nil {
		store.DefaultSubStore.Start()
		log.Info("sub store[%s] started", store.DefaultSubStore.Name())

		this.subServer.Start()
	}

	if options.consulAddr != "" {
		if err := registry.Default.Register(); err != nil {
			panic(err)
		}
	}

	log.Info("gateway[%s:%s] ready", ctx.Hostname(), this.id)

	return nil
}

func (this *Gateway) Stop() {
	this.shutdownOnce.Do(func() {
		log.Info("stopping kateway...")

		close(this.shutdownCh)
	})

}

func (this *Gateway) ServeForever() {
	select {
	case <-this.shutdownCh:
		if options.consulAddr != "" {
			registry.Default.Deregister()
		}

		this.wg.Wait()

		if store.DefaultPubStore != nil {
			store.DefaultPubStore.Stop()
		}
		if store.DefaultSubStore != nil {
			store.DefaultSubStore.Stop()
		}

		log.Trace("all components shutdown complete")

		meta.Default.Stop()
		this.timer.Stop()
	}

}
