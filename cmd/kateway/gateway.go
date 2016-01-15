package main

import (
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/meta/zkmeta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/cmd/kateway/store/dumb"
	"github.com/funkygao/gafka/cmd/kateway/store/kafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/registry"
	"github.com/funkygao/gafka/registry/zk"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/golib/ratelimiter"
	"github.com/funkygao/golib/signal"
	"github.com/funkygao/golib/timewheel"
	log "github.com/funkygao/log4go"
)

// Gateway is a distributed kafka Pub/Sub HTTP endpoint.
type Gateway struct {
	id        string
	startedAt time.Time
	zone      string

	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	wg           sync.WaitGroup

	certFile string
	keyFile  string

	// online http producers client
	producers    map[string]Producer // key is remote addr
	produersLock sync.RWMutex

	// online http consumers client
	consumers     map[string]Consumer // key is remote addr
	consumersLock sync.RWMutex

	// cluster of kateway pub, only for client side load balance
	pubPeers     []string
	pubPeersLock sync.RWMutex

	zkzone *gzk.ZkZone

	pubServer *pubServer
	subServer *subServer
	manServer *manServer

	pubMetrics *pubMetrics
	subMetrics *subMetrics
	svrMetrics *serverMetrics

	guard        *guard
	timer        *timewheel.TimeWheel
	leakyBuckets *ratelimiter.LeakyBuckets // TODO
}

func NewGateway(id string, metaRefreshInterval time.Duration) *Gateway {
	this := &Gateway{
		id:           id,
		zone:         options.Zone,
		shutdownCh:   make(chan struct{}),
		leakyBuckets: ratelimiter.NewLeakyBuckets(1000*60, time.Minute),
		certFile:     options.CertFile,
		keyFile:      options.KeyFile,
		pubPeers:     make([]string, 0, 20),
		producers:    make(map[string]Producer, 100),
		consumers:    make(map[string]Consumer, 100),
	}

	registry.Default = zk.New(this.zone, this.id, this.InstanceInfo())

	metaConf := zkmeta.DefaultConfig(this.zone)
	metaConf.Refresh = metaRefreshInterval
	meta.Default = zkmeta.New(metaConf)
	this.guard = newGuard(this)
	this.timer = timewheel.NewTimeWheel(time.Second, 120)

	this.manServer = newManServer(options.ManHttpAddr, options.ManHttpsAddr,
		options.MaxClients, this)
	this.svrMetrics = NewServerMetrics(options.ReporterInterval, this)

	if options.PubHttpAddr != "" || options.PubHttpsAddr != "" {
		this.pubServer = newPubServer(options.PubHttpAddr, options.PubHttpsAddr,
			options.MaxClients, this)
		this.pubMetrics = NewPubMetrics(this)

		switch options.Store {
		case "kafka":
			store.DefaultPubStore = kafka.NewPubStore(
				options.PubPoolCapcity, options.MaxPubRetries, options.PubPoolIdleTimeout,
				&this.wg, options.Debug, options.DryRun)

		case "dumb":
			store.DefaultPubStore = dumb.NewPubStore(&this.wg, options.Debug)
		}
	}

	if options.SubHttpAddr != "" || options.SubHttpsAddr != "" {
		this.subServer = newSubServer(options.SubHttpAddr, options.SubHttpsAddr,
			options.MaxClients, this)
		this.subMetrics = NewSubMetrics(this)

		switch options.Store {
		case "kafka":
			store.DefaultSubStore = kafka.NewSubStore(&this.wg,
				this.subServer.closedConnCh, options.Debug)

		case "dumb":
			store.DefaultSubStore = dumb.NewSubStore(&this.wg,
				this.subServer.closedConnCh, options.Debug)

		}
	}

	return this
}

func (this *Gateway) InstanceInfo() []byte {
	var s map[string]string = make(map[string]string)
	s["host"] = ctx.Hostname()
	s["id"] = this.id
	s["ver"] = gafka.Version
	s["build"] = gafka.BuildId
	s["zone"] = this.zone
	s["man"] = options.ManHttpAddr
	s["sman"] = options.ManHttpsAddr
	s["pub"] = options.PubHttpAddr
	s["spub"] = options.PubHttpsAddr
	s["sub"] = options.SubHttpAddr
	s["ssub"] = options.SubHttpsAddr
	s["debug"] = options.DebugHttpAddr
	d, _ := json.Marshal(s)
	return d
}

func (this *Gateway) GetZkZone() *gzk.ZkZone {
	if this.zkzone == nil {
		this.zkzone = gzk.NewZkZone(gzk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	}

	return this.zkzone
}

func (this *Gateway) Start() (err error) {
	signal.RegisterSignalsHandler(func(sig os.Signal) {
		this.Stop()
	}, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2) // yes we ignore HUP

	this.startedAt = time.Now()

	meta.Default.Start()
	log.Trace("meta store started")

	this.guard.Start()
	log.Trace("guard started")

	this.buildRouting()
	this.manServer.Start()

	if options.DebugHttpAddr != "" {
		log.Info("debug http server ready on %s", options.DebugHttpAddr)

		go http.ListenAndServe(options.DebugHttpAddr, nil)
	}

	this.svrMetrics.Load()

	if this.pubServer != nil {
		if err := store.DefaultPubStore.Start(); err != nil {
			panic(err)
		}
		log.Trace("pub store[%s] started", store.DefaultPubStore.Name())

		this.pubMetrics.Load()
		this.pubServer.Start()
	}
	if this.subServer != nil {
		if err := store.DefaultSubStore.Start(); err != nil {
			panic(err)
		}
		log.Trace("sub store[%s] started", store.DefaultSubStore.Name())

		this.subMetrics.Load()
		this.subServer.Start()
	}

	go startRuntimeMetrics(options.ReporterInterval)

	// the last thing is to register: notify others
	if registry.Default != nil {
		if err := registry.Default.Register(); err != nil {
			panic(err)
		}

		log.Info("gateway[%s:%s] ready, registered in %s", ctx.Hostname(), this.id,
			registry.Default.Name())
	} else {
		log.Info("gateway[%s:%s] ready, unregistered", ctx.Hostname(), this.id)
	}

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
		// the 1st thing is to deregister
		if registry.Default != nil {
			if err := registry.Default.Deregister(); err != nil {
				log.Error("Deregister: %v", err)
			}
		}

		log.Info("Deregister done, waiting for components shutdown...")
		this.wg.Wait()

		if store.DefaultPubStore != nil {
			store.DefaultPubStore.Stop()
		}
		if store.DefaultSubStore != nil {
			store.DefaultSubStore.Stop()
		}

		log.Info("all components shutdown complete")

		this.svrMetrics.Flush()
		log.Info("server metrics flushed")
		if this.pubMetrics != nil {
			this.pubMetrics.Flush()
			log.Info("pub metrics flushed")
		}
		if this.subMetrics != nil {
			this.subMetrics.Flush()
			log.Info("sub metrics flushed")
		}

		if this.zkzone != nil {
			this.zkzone.Close()
		}

		meta.Default.Stop()
		this.timer.Stop()
	}

}
