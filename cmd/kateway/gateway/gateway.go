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

	_ "expvar" // register /debug/vars HTTP handler

	"github.com/funkygao/fae/config"
	"github.com/funkygao/gafka"
	"github.com/funkygao/gafka/cmd/kateway/hh"
	hhdisk "github.com/funkygao/gafka/cmd/kateway/hh/disk"
	"github.com/funkygao/gafka/cmd/kateway/job"
	jobdummy "github.com/funkygao/gafka/cmd/kateway/job/dummy"
	jobmysql "github.com/funkygao/gafka/cmd/kateway/job/mysql"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	mandummy "github.com/funkygao/gafka/cmd/kateway/manager/dummy"
	mandb "github.com/funkygao/gafka/cmd/kateway/manager/mysql"
	manopen "github.com/funkygao/gafka/cmd/kateway/manager/open"
	"github.com/funkygao/gafka/cmd/kateway/meta"
	"github.com/funkygao/gafka/cmd/kateway/meta/zkmeta"
	"github.com/funkygao/gafka/cmd/kateway/store"
	storedummy "github.com/funkygao/gafka/cmd/kateway/store/dummy"
	storekfk "github.com/funkygao/gafka/cmd/kateway/store/kafka"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/registry"
	"github.com/funkygao/gafka/registry/zk"
	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/gafka/telemetry/influxdb"
	gzk "github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	"github.com/funkygao/golib/signal"
	"github.com/funkygao/golib/timewheel"
	log "github.com/funkygao/log4go"
	zklib "github.com/samuel/go-zookeeper/zk"
)

// Gateway is a distributed Pub/Sub HTTP endpoint.
//
// Working with ehaproxy, it can form a Pub/Sub cluster system.
type Gateway struct {
	id string // must be unique across the zone

	zkzone       *gzk.ZkZone // load/resume/flush counter metrics to zk
	svrMetrics   *serverMetrics
	accessLogger *AccessLogger
	timer        *timewheel.TimeWheel

	shutdownOnce        sync.Once
	shutdownCh, quiting chan struct{}
	wg                  sync.WaitGroup

	certFile string
	keyFile  string

	pubServer *pubServer
	subServer *subServer
	manServer *manServer
	debugMux  *http.ServeMux
}

func New(id string) *Gateway {
	this := &Gateway{
		id:         id,
		shutdownCh: make(chan struct{}),
		quiting:    make(chan struct{}),
		certFile:   Options.CertFile,
		keyFile:    Options.KeyFile,
	}

	this.zkzone = gzk.NewZkZone(gzk.DefaultConfig(Options.Zone, ctx.ZoneZkAddrs(Options.Zone)))
	if err := this.zkzone.Ping(); err != nil {
		panic(err)
	}

	if Options.EnableRegistry {
		registry.Default = zk.New(this.zkzone, this.id, this.InstanceInfo())
	}
	metaConf := zkmeta.DefaultConfig()
	metaConf.Refresh = Options.MetaRefresh
	meta.Default = zkmeta.New(metaConf, this.zkzone)
	this.timer = timewheel.NewTimeWheel(time.Second, 120)
	this.accessLogger = NewAccessLogger("access_log", 100)
	this.svrMetrics = NewServerMetrics(Options.ReporterInterval, this)
	rc, err := influxdb.NewConfig(Options.InfluxServer, Options.InfluxDbName, "", "", Options.ReporterInterval)
	if err != nil {
		log.Error("telemetry: %v", err)
	} else {
		telemetry.Default = influxdb.New(metrics.DefaultRegistry, rc)
	}
	switch Options.HintedHandoffType {
	case "disk":
		cfg := hhdisk.DefaultConfig()
		cfg.Dir = Options.HintedHandoffDir
		hh.Default = hhdisk.New(cfg)
	}

	// initialize the manager store
	switch Options.ManagerStore {
	case "mysql":
		cf := mandb.DefaultConfig(Options.Zone)
		cf.Refresh = Options.ManagerRefresh
		manager.Default = mandb.New(cf)
		manager.Default.AllowSubWithUnregisteredGroup(Options.PermitUnregisteredGroup)

	case "dummy":
		manager.Default = mandummy.New(Options.DummyCluster)

	case "open":
		cf := manopen.DefaultConfig(Options.Zone)
		cf.Refresh = Options.ManagerRefresh
		manager.Default = manopen.New(cf)
		manager.Default.AllowSubWithUnregisteredGroup(Options.PermitUnregisteredGroup)
		HttpHeaderAppid = "devid"
		HttpHeaderPubkey = "devsecret"
		HttpHeaderSubkey = "devsecret"

	default:
		panic("invalid manager store:" + Options.ManagerStore)
	}

	// initialize the servers on demand
	if Options.DebugHttpAddr != "" {
		this.debugMux = http.NewServeMux()
	}
	if Options.ManHttpAddr != "" || Options.ManHttpsAddr != "" {
		this.manServer = newManServer(Options.ManHttpAddr, Options.ManHttpsAddr,
			Options.MaxClients, this)
	} else {
		panic("manager server must be present")
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
			panic("invalid message store")
		}

		switch Options.JobStore {
		case "mysql":
			var mcc = &config.ConfigMysql{}
			b, err := this.zkzone.KatewayJobClusterConfig()
			if err != nil {
				panic(err)
			}
			if err = mcc.From(b); err != nil {
				panic(err)
			}
			jm, err := jobmysql.New(id, mcc)
			if err != nil {
				panic(err)
			}

			job.Default = jm

		case "dummy":
			job.Default = jobdummy.New()

		default:
			panic("invalid job store")
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
	ip, err := ctx.LocalIP()
	if err != nil {
		panic(err)
	}
	info := gzk.KatewayMeta{
		Id:        this.id,
		Zone:      Options.Zone,
		Ver:       gafka.Version,
		Build:     gafka.BuildId,
		BuiltAt:   gafka.BuiltAt,
		Host:      ctx.Hostname(),
		Ip:        ip.String(),
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
	log.Info("starting gateway[%s@%s]...", gafka.BuildId, gafka.BuiltAt)

	signal.RegisterSignalsHandler(func(sig os.Signal) {
		this.shutdownOnce.Do(func() {
			log.Info("gateway[%s@%s] received signal: %s", gafka.BuildId, gafka.BuiltAt, strings.ToUpper(sig.String()))

			close(this.quiting)
		})
	}, syscall.SIGINT, syscall.SIGTERM) // yes we ignore HUP

	// keep watch on zk connection jitter
	go func() {
		evtCh, ok := this.zkzone.SessionEvents()
		if !ok {
			log.Error("someone else is stealing my zk events?")
			return
		}

		// during connecting phase, the following events are fired:
		// StateConnecting -> StateConnected -> StateHasSession
		firstHandShaked := false
		for {
			select {
			case <-this.shutdownCh:
				return

			case evt, ok := <-evtCh:
				if !ok {
					return
				}

				if !firstHandShaked {
					if evt.State == zklib.StateHasSession {
						firstHandShaked = true
					}

					continue
				}

				log.Warn("zk jitter: %+v", evt)

				if evt.State == zklib.StateHasSession {
					log.Warn("zk reconnected after session lost, watcher/ephemeral lost")

					this.zkzone.CallSOS(fmt.Sprintf("kateway[%s]", this.id), "zk session expired")
				}
			}
		}
	}()

	meta.Default.Start()
	log.Trace("meta store[%s] started", meta.Default.Name())

	if err = manager.Default.Start(); err != nil {
		return
	}
	log.Trace("manager store[%s] started", manager.Default.Name())

	if telemetry.Default != nil {
		go func() {
			log.Trace("telemetry[%s] started", telemetry.Default.Name())

			if err = telemetry.Default.Start(); err != nil {
				log.Error("telemetry[%s]: %v", telemetry.Default.Name(), err)
			}
		}()
	}

	if Options.EnableAccessLog {
		if err = this.accessLogger.Start(); err != nil {
			log.Error("access logger: %s", err)
		}
	}

	this.buildRouting()

	this.svrMetrics.Load()
	go startRuntimeMetrics(Options.ReporterInterval)

	// start up the servers
	this.manServer.Start() // man server is always present
	if this.pubServer != nil {
		if err = store.DefaultPubStore.Start(); err != nil {
			panic(err)
		}
		log.Trace("pub store[%s] started", store.DefaultPubStore.Name())

		if err = hh.Default.Start(); err != nil {
			return
		}
		log.Trace("hh[%s] started", hh.Default.Name())

		if err = job.Default.Start(); err != nil {
			panic(err)
		}
		log.Trace("job store[%s] started", job.Default.Name())

		this.pubServer.Start()
	}
	if this.subServer != nil {
		if err = store.DefaultSubStore.Start(); err != nil {
			panic(err)
		}
		log.Trace("sub store[%s] started", store.DefaultSubStore.Name())

		this.subServer.Start()
	}

	// the last thing is to register: notify others: come on baby!
	if registry.Default != nil {
		if err = registry.Default.Register(); err != nil {
			panic(err)
		}

		log.Info("gateway[%s:%s] ready, registered in %s :-)", ctx.Hostname(), this.id,
			registry.Default.Name())
	} else {
		log.Info("gateway[%s:%s] ready, unregistered", ctx.Hostname(), this.id)
	}

	return nil
}

func (this *Gateway) ServeForever() {
	select {
	case <-this.quiting:
		// the 1st thing is to deregister
		if registry.Default != nil {
			if err := registry.Default.Deregister(); err != nil {
				log.Error("de-register: %v", err)
			} else {
				log.Info("de-registered from %s", registry.Default.Name())
			}
		}

		close(this.shutdownCh)

		// store can only be closed after web server closed
		if this.pubServer != nil {
			log.Trace("awaiting pub server stop...")
			<-this.pubServer.Closed()
		}
		if this.subServer != nil {
			log.Trace("awaiting sub server stop...")
			<-this.subServer.Closed()
		}
		<-this.manServer.Closed()

		if hh.Default != nil {
			log.Trace("hh[%s] stop...", hh.Default.Name())
			hh.Default.Stop()
			log.Trace("hh[%s] flush inflights...", hh.Default.Name())
			hh.Default.FlushInflights()
		}

		//log.Trace("stopping access logger")
		//this.accessLogger.Stop() FIXME it will hang on linux

		if store.DefaultPubStore != nil {
			log.Trace("pub store[%s] stop...", store.DefaultPubStore.Name())
			go store.DefaultPubStore.Stop()
		}
		if store.DefaultSubStore != nil {
			log.Trace("sub store[%s] stop...", store.DefaultSubStore.Name())
			go store.DefaultSubStore.Stop()
		}
		if job.Default != nil {
			job.Default.Stop()
			log.Trace("job store[%s] stopped", job.Default.Name())
		}

		log.Info("...waiting for services shutdown...")
		this.wg.Wait()
		log.Info("<----- all services shutdown ----->")

		this.svrMetrics.Flush()
		log.Trace("svr metrics flushed")

		if telemetry.Default != nil {
			telemetry.Default.Stop()
			log.Trace("telemetry[%s] stopped", telemetry.Default.Name())
		}

		meta.Default.Stop()
		log.Trace("meta store[%s] stopped", meta.Default.Name())

		manager.Default.Stop()
		log.Trace("manager store[%s] stopped", manager.Default.Name())

		if this.zkzone != nil {
			this.zkzone.Close()
			log.Trace("zkzone stopped")
		}

		this.timer.Stop()
	}

}
