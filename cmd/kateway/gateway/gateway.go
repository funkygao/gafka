package gateway

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
	"github.com/funkygao/gafka/cmd/kateway/manager"
	mandummy "github.com/funkygao/gafka/cmd/kateway/manager/dummy"
	mandb "github.com/funkygao/gafka/cmd/kateway/manager/mysql"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/meta/zkmeta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	storedummy "github.com/funkygao/gafka/cmd/kateway/store/dummy"
	storekfk "github.com/funkygao/gafka/cmd/kateway/store/kafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/registry"
	"github.com/funkygao/gafka/registry/zk"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/golib/signal"
	"github.com/funkygao/golib/timewheel"
	log "github.com/funkygao/log4go"
)

// Gateway is a distributed kafka Pub/Sub HTTP endpoint.
type Gateway struct {
	id     string // must be unique across the cluster
	zone   string
	zkzone *gzk.ZkZone // load/resume/flush counter metrics to zk

	startedAt    time.Time
	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	wg           sync.WaitGroup

	certFile string
	keyFile  string

	pubServer *pubServer
	subServer *subServer
	manServer *manServer

	clientStates *ClientStates

	connections   map[string]int // remoteAddr:counter
	connectionsMu sync.Mutex

	svrMetrics *serverMetrics

	accessLogger *AccessLogger
	guard        *guard
	timer        *timewheel.TimeWheel
}

func NewGateway(id string, metaRefreshInterval time.Duration) *Gateway {
	this := &Gateway{
		id:           id,
		zone:         Options.Zone,
		shutdownCh:   make(chan struct{}),
		certFile:     Options.CertFile,
		keyFile:      Options.KeyFile,
		clientStates: NewClientStates(),
		connections:  make(map[string]int, 1000),
	}

	registry.Default = zk.New(this.zone, this.id, this.InstanceInfo())

	metaConf := zkmeta.DefaultConfig(this.zone)
	metaConf.Refresh = metaRefreshInterval
	meta.Default = zkmeta.New(metaConf)
	this.guard = newGuard(this)
	this.timer = timewheel.NewTimeWheel(time.Second, 120)

	this.accessLogger = NewAccessLogger("access_log", 100)

	this.manServer = newManServer(Options.ManHttpAddr, Options.ManHttpsAddr,
		Options.MaxClients, this)
	this.svrMetrics = NewServerMetrics(Options.ReporterInterval, this)

	switch Options.ManagerStore {
	case "mysql":
		cf := mandb.DefaultConfig(this.zone)
		cf.Refresh = Options.ManagerRefresh
		manager.Default = mandb.New(cf)
		manager.Default.AllowSubWithUnregisteredGroup(Options.PermitUnregisteredGroup)

	case "dummy":
		manager.Default = mandummy.New()

	default:
		panic("invalid manager")
	}

	if Options.PubHttpAddr != "" || Options.PubHttpsAddr != "" {
		this.pubServer = newPubServer(Options.PubHttpAddr, Options.PubHttpsAddr,
			Options.MaxClients, this)

		switch Options.Store {
		case "kafka":
			store.DefaultPubStore = storekfk.NewPubStore(
				Options.PubPoolCapcity, Options.MaxPubRetries, Options.PubPoolIdleTimeout,
				&this.wg, Options.Debug, Options.DryRun)

		case "dummy":
			store.DefaultPubStore = storedummy.NewPubStore(&this.wg, Options.Debug)

		default:
			panic("invalid store")
		}
	}

	if Options.SubHttpAddr != "" || Options.SubHttpsAddr != "" {
		this.subServer = newSubServer(Options.SubHttpAddr, Options.SubHttpsAddr,
			Options.MaxClients, this)

		switch Options.Store {
		case "kafka":
			store.DefaultSubStore = storekfk.NewSubStore(&this.wg,
				this.subServer.closedConnCh, Options.Debug)

		case "dummy":
			store.DefaultSubStore = storedummy.NewSubStore(&this.wg,
				this.subServer.closedConnCh, Options.Debug)

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
		PubAddr:   Options.PubHttpAddr,
		SPubAddr:  Options.PubHttpsAddr,
		SubAddr:   Options.SubHttpAddr,
		SSubAddr:  Options.SubHttpsAddr,
		ManAddr:   Options.ManHttpAddr,
		SManAddr:  Options.ManHttpsAddr,
		DebugAddr: Options.DebugHttpAddr,
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
			log.Error("deregister: %v", err)
			return
		}

		log.Info("deregistered from %s", registry.Default.Name())
	}, syscall.SIGUSR1) // disallow load balancer dispatch to me

	this.startedAt = time.Now()

	meta.Default.Start()
	log.Trace("meta store[%s] started", meta.Default.Name())

	if err = manager.Default.Start(); err != nil {
		return
	}
	log.Trace("manager store[%s] started", manager.Default.Name())

	this.guard.Start()
	log.Trace("guard started")

	if Options.EnableAccessLog {
		if err = this.accessLogger.Start(); err != nil {
			log.Error("access logger: %s", err)
		}
	}

	this.buildRouting()
	this.manServer.Start()

	if Options.DebugHttpAddr != "" {
		log.Info("debug http server ready on %s", Options.DebugHttpAddr)

		go http.ListenAndServe(Options.DebugHttpAddr, nil)
	}

	this.svrMetrics.Load()

	if this.pubServer != nil {
		if err := store.DefaultPubStore.Start(); err != nil {
			panic(err)
		}
		log.Trace("pub store[%s] started", store.DefaultPubStore.Name())

		this.pubServer.Start()
	}
	if this.subServer != nil {
		if err := store.DefaultSubStore.Start(); err != nil {
			panic(err)
		}
		log.Trace("sub store[%s] started", store.DefaultSubStore.Name())

		this.subServer.Start()
	}

	go startRuntimeMetrics(Options.ReporterInterval)

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

		log.Info("deregistered from %s", registry.Default.Name())
		log.Info("waiting for servers shutdown...")
		this.wg.Wait()
		log.Info("<----- all servers shutdown ----->")

		this.accessLogger.Stop()

		if store.DefaultPubStore != nil {
			store.DefaultPubStore.Stop()
		}
		if store.DefaultSubStore != nil {
			store.DefaultSubStore.Stop()
		}

		this.svrMetrics.Flush()

		meta.Default.Stop()
		log.Trace("meta store stopped")
		manager.Default.Stop()
		log.Trace("manager store stopped")

		if this.zkzone != nil {
			this.zkzone.Close()
			log.Trace("zkzone stopped")
		}

		this.timer.Stop()
	}

}
