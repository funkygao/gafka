package main

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "expvar"
	_ "net/http/pprof"

	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kateway/inflights"
	"github.com/funkygao/gafka/cmd/kateway/inflights/mem"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	metadummy "github.com/funkygao/gafka/cmd/kateway/manager/dummy"
	"github.com/funkygao/gafka/cmd/kateway/manager/mysql"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/meta/zkmeta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	storedummy "github.com/funkygao/gafka/cmd/kateway/store/dummy"
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

	zkzone *gzk.ZkZone // load/resume/flush counter metrics to zk

	pubServer *pubServer
	subServer *subServer
	manServer *manServer

	clientStates *ClientStates

	pubMetrics *pubMetrics
	subMetrics *subMetrics
	svrMetrics *serverMetrics

	accessLogger      *AccessLogger
	guard             *guard
	timer             *timewheel.TimeWheel
	throttlePub       *ratelimiter.LeakyBuckets
	throttleAddTopic  *ratelimiter.LeakyBuckets
	throttleSubStatus *ratelimiter.LeakyBuckets
}

func NewGateway(id string, metaRefreshInterval time.Duration) *Gateway {
	this := &Gateway{
		id:                id,
		zone:              options.Zone,
		shutdownCh:        make(chan struct{}),
		throttlePub:       ratelimiter.NewLeakyBuckets(10000*60, time.Minute), // TODO
		throttleAddTopic:  ratelimiter.NewLeakyBuckets(60, time.Minute),
		throttleSubStatus: ratelimiter.NewLeakyBuckets(60, time.Minute),
		certFile:          options.CertFile,
		keyFile:           options.KeyFile,
		clientStates:      NewClientStates(),
	}

	registry.Default = zk.New(this.zone, this.id, this.InstanceInfo())
	inflights.Default = mem.New(options.InflightsSnapshot)

	metaConf := zkmeta.DefaultConfig(this.zone)
	metaConf.Refresh = metaRefreshInterval
	meta.Default = zkmeta.New(metaConf)
	this.guard = newGuard(this)
	this.timer = timewheel.NewTimeWheel(time.Second, 120)

	this.accessLogger = NewAccessLogger("access_log", 100)

	this.manServer = newManServer(options.ManHttpAddr, options.ManHttpsAddr,
		options.MaxClients, this)
	this.svrMetrics = NewServerMetrics(options.ReporterInterval, this)

	switch options.ManagerStore {
	case "mysql":
		managerCf := mysql.DefaultConfig(this.zone)
		managerCf.Refresh = options.ManagerRefresh
		manager.Default = mysql.New(managerCf)

	case "dummy":
		manager.Default = metadummy.New()

	default:
		panic("invalid manager")
	}

	if options.PubHttpAddr != "" || options.PubHttpsAddr != "" {
		this.pubServer = newPubServer(options.PubHttpAddr, options.PubHttpsAddr,
			options.MaxClients, this)
		this.pubMetrics = NewPubMetrics(this)

		switch options.Store {
		case "kafka":
			store.DefaultPubStore = kafka.NewPubStore(
				options.PubPoolCapcity, options.MaxPubRetries, options.PubPoolIdleTimeout,
				&this.wg, options.Debug, options.DryRun)

		case "dummy":
			store.DefaultPubStore = storedummy.NewPubStore(&this.wg, options.Debug)

		default:
			panic("invalid store")
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

		case "dummy":
			store.DefaultSubStore = storedummy.NewSubStore(&this.wg,
				this.subServer.closedConnCh, options.Debug)

		}
	}

	return this
}

func (this *Gateway) InstanceInfo() []byte {
	info := gzk.KatewayMeta{
		Id:        this.id,
		Zone:      this.zone,
		Ver:       gafka.Version,
		Build:     gafka.BuildId,
		BuiltAt:   gafka.BuiltAt,
		Host:      ctx.Hostname(),
		Cpu:       ctx.NumCPUStr(),
		PubAddr:   options.PubHttpAddr,
		SPubAddr:  options.PubHttpsAddr,
		SubAddr:   options.SubHttpAddr,
		SSubAddr:  options.SubHttpsAddr,
		ManAddr:   options.ManHttpAddr,
		SManAddr:  options.ManHttpsAddr,
		DebugAddr: options.DebugHttpAddr,
	}
	d, _ := json.Marshal(info)
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
		log.Info("received signal: %s", strings.ToUpper(sig.String()))
		this.Stop()
	}, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR2) // yes we ignore HUP

	// FIXME load balancer will redispatch the consumer to another
	// kateway, but the offset has not been committed, then???
	signal.RegisterSignalsHandler(func(sig os.Signal) {
		if registry.Default == nil {
			log.Warn("USR1 fired when no registry defined")
			return
		}

		if err := registry.Default.Deregister(); err != nil {
			log.Error("Deregister: %v", err)
			return
		}

		log.Info("Deregister done")
	}, syscall.SIGUSR1) // disallow load balancer dispatch to me

	this.startedAt = time.Now()

	if err = inflights.Default.Init(); err != nil {
		log.Error("inflights init: %v", err)
	}

	meta.Default.Start()
	log.Trace("meta store[%s] started", meta.Default.Name())

	manager.Default.Start()
	log.Trace("manager store[%s] started", manager.Default.Name())

	this.guard.Start()
	log.Trace("guard started")

	if options.EnableAccessLog {
		if err = this.accessLogger.Start(); err != nil {
			log.Error("access logger: %s", err)
		}
	}

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

		this.accessLogger.Stop()
		log.Trace("access logger closed")

		inflights.Default.Stop()

		if store.DefaultPubStore != nil {
			store.DefaultPubStore.Stop()
		}
		if store.DefaultSubStore != nil {
			store.DefaultSubStore.Stop()
		}

		log.Info("all components shutdown complete")

		this.svrMetrics.Flush()
		log.Trace("server metrics flushed")
		if this.pubMetrics != nil {
			this.pubMetrics.Flush()
			log.Trace("pub metrics flushed")
		}
		if this.subMetrics != nil {
			this.subMetrics.Flush()
			log.Trace("sub metrics flushed")
		}

		meta.Default.Stop()
		log.Trace("meta store[%s] stopped", meta.Default.Name())

		manager.Default.Stop()
		log.Trace("manager store[%s] stopped", manager.Default.Name())

		if this.zkzone != nil {
			this.zkzone.Close()
		}

		this.timer.Stop()
	}

}
