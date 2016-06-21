package gateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
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

// Gateway is a distributed Pub/Sub HTTP endpoint.
//
// Working with ehaproxy, it can form a Pub/Sub cluster system.
type Gateway struct {
	id string // must be unique across the zone

	zkzone       *gzk.ZkZone // load/resume/flush counter metrics to zk
	svrMetrics   *serverMetrics
	accessLogger *AccessLogger
	guard        *guard
	timer        *timewheel.TimeWheel

	shutdownOnce sync.Once
	shutdownCh   chan struct{}
	wg           sync.WaitGroup

	certFile string
	keyFile  string

	pubServer *pubServer
	subServer *subServer
	manServer *manServer

	clientStates *ClientStates
}

func New(id string) *Gateway {
	this := &Gateway{
		id:           id,
		shutdownCh:   make(chan struct{}),
		certFile:     Options.CertFile,
		keyFile:      Options.KeyFile,
		clientStates: NewClientStates(),
	}

	this.zkzone = gzk.NewZkZone(gzk.DefaultConfig(Options.Zone, ctx.ZoneZkAddrs(Options.Zone)))
	if err := this.zkzone.Ping(); err != nil {
		panic(err)
	}

	if Options.EnableRegistry {
		registry.Default = zk.New(Options.Zone, this.id, this.InstanceInfo())
	}
	metaConf := zkmeta.DefaultConfig()
	metaConf.Refresh = Options.MetaRefresh
	meta.Default = zkmeta.New(metaConf, this.zkzone)
	this.guard = newGuard(this)
	this.timer = timewheel.NewTimeWheel(time.Second, 120)
	this.accessLogger = NewAccessLogger("access_log", 100)
	this.svrMetrics = NewServerMetrics(Options.ReporterInterval, this)

	// initialize the manager store
	switch Options.ManagerStore {
	case "mysql":
		cf := mandb.DefaultConfig(Options.Zone)
		cf.Refresh = Options.ManagerRefresh
		manager.Default = mandb.New(cf)
		manager.Default.AllowSubWithUnregisteredGroup(Options.PermitUnregisteredGroup)

	case "dummy":
		manager.Default = mandummy.New()

	default:
		panic("invalid manager store")
	}

	// initialize the servers on demand
	if Options.ManHttpAddr != "" || Options.ManHttpsAddr != "" {
		this.manServer = newManServer(Options.ManHttpAddr, Options.ManHttpsAddr,
			Options.MaxClients, this)
	}
	if Options.PubHttpAddr != "" || Options.PubHttpsAddr != "" {
		this.pubServer = newPubServer(Options.PubHttpAddr, Options.PubHttpsAddr,
			Options.MaxClients, this)

		switch Options.Store {
		case "kafka":
			store.DefaultPubStore = storekfk.NewPubStore(Options.PubPoolCapcity, Options.PubPoolIdleTimeout,
				Options.UseCompress, &this.wg, Options.Debug, Options.DryRun)

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

		default:
			panic("invalid store")

		}
	}

	return this
}

func (this *Gateway) InstanceInfo() []byte {
	info := gzk.KatewayMeta{
		Id:        this.id,
		Zone:      Options.Zone,
		Ver:       gafka.Version,
		Build:     gafka.BuildId,
		BuiltAt:   gafka.BuiltAt,
		Host:      ctx.Hostname(),
		Cpu:       ctx.NumCPUStr(),
		Arch:      fmt.Sprintf("%s:%s-%s/%s", runtime.Compiler, runtime.Version(), runtime.GOOS, runtime.GOARCH),
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

func (this *Gateway) Start() (err error) {
	log.Trace("starting gateway...")

	signal.RegisterSignalsHandler(func(sig os.Signal) {
		log.Info("received signal: %s", strings.ToUpper(sig.String()))
		this.stop()
	}, syscall.SIGINT, syscall.SIGTERM) // yes we ignore HUP

	meta.Default.Start()
	log.Trace("meta store[%s] started", meta.Default.Name())

	if err = manager.Default.Start(); err != nil {
		return
	}
	log.Trace("manager store[%s] started", manager.Default.Name())

	go this.watchDeadPartitions()

	go this.guard.Start()
	log.Trace("guard started")

	if Options.EnableAccessLog {
		if err = this.accessLogger.Start(); err != nil {
			log.Error("access logger: %s", err)
		}
	}

	this.buildRouting()

	if Options.DebugHttpAddr != "" {
		log.Info("debug http server ready on %s", Options.DebugHttpAddr)

		go http.ListenAndServe(Options.DebugHttpAddr, nil)
	}

	this.svrMetrics.Load()
	go startRuntimeMetrics(Options.ReporterInterval)

	// start up the servers
	if this.manServer != nil {
		this.manServer.Start()
	}
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

	// the last thing is to register: notify others: come on baby!
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

func (this *Gateway) stop() {
	this.shutdownOnce.Do(func() {
		log.Info("stopping gateway...")

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

			log.Info("deregistered from %s", registry.Default.Name())
		}

		log.Info("waiting for servers shutdown...")
		this.wg.Wait()
		log.Info("<----- all servers shutdown ----->")

		this.accessLogger.Stop()

		log.Trace("stopping pub store")
		if store.DefaultPubStore != nil {
			store.DefaultPubStore.Stop()
		}
		log.Trace("stopping sub store")
		if store.DefaultSubStore != nil {
			store.DefaultSubStore.Stop()
		}

		this.svrMetrics.Flush()
		log.Trace("svr metrics flushed")

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
