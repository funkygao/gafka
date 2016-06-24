package monitor

import (
	"flag"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/leadership"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/reporter"
	"github.com/funkygao/gafka/reporter/influxdb"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
	"github.com/rcrowley/go-metrics"
)

// Monitor is the engine that will start/stop plugin watchers.
// It itself is an implementation of Context.
type Monitor struct {
	influxdbAddr   string
	influxdbDbName string
	apiAddr        string

	router *httprouter.Router
	zkzone *zk.ZkZone

	candidate *leadership.Candidate

	wg     *sync.WaitGroup
	stop   chan struct{} // broadcast to all watchers to stop, but might restart again
	quit   chan struct{}
	leader bool
}

func (this *Monitor) Init() {
	var logFile, zone string
	flag.StringVar(&logFile, "log", "stdout", "log filename")
	flag.StringVar(&zone, "z", "", "zone, required")
	flag.StringVar(&this.apiAddr, "http", ":10025", "api http server addr")
	flag.StringVar(&this.influxdbAddr, "influxAddr", "", "influxdb addr, required")
	flag.StringVar(&this.influxdbDbName, "db", "", "influxdb db name, required")
	flag.Parse()

	if zone == "" || this.influxdbDbName == "" || this.influxdbAddr == "" {
		panic("run help ")
	}

	ctx.LoadFromHome()
	this.zkzone = zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	this.quit = make(chan struct{})

	// export RESTful api
	this.router = httprouter.New()
	this.router.GET("/metrics", this.metricsHandler)

	if logFile == "stdout" {
		log.AddFilter("stdout", log.INFO, log.NewConsoleLogWriter())
	} else {
		log.DeleteFilter("stdout")

		filer := log.NewFileLogWriter(logFile, true, false, 0644)
		filer.SetFormat("[%d %T] [%L] (%S) %M")
		log.AddFilter("file", log.INFO, filer)
	}

	reporterConfig, err := influxdb.NewConfig(this.influxdbAddr, this.influxdbDbName, "", "", time.Minute)
	if err != nil {
		panic(err)
	}
	reporter.Default = influxdb.New(metrics.DefaultRegistry, reporterConfig)
}

func (this *Monitor) Stop() {
	if this.leader {
		this.leader = false

		log.Info("stopping all watchers ...")
		close(this.stop)

		log.Info("stopping reporter...")
		reporter.Default.Stop()
	}
}

func (this *Monitor) Start() {
	this.leader = true

	this.stop = make(chan struct{})

	go func() {
		if err := reporter.Default.Start(); err != nil {
			log.Error("reporter: %v", err)
		}
	}()

	this.wg = new(sync.WaitGroup)
	for name, watcherFactory := range registeredWatchers {
		watcher := watcherFactory()
		watcher.Init(this)

		log.Info("created and starting watcher: %s", name)

		this.wg.Add(1)
		go watcher.Run()
	}

	log.Info("all watchers ready!")

	<-this.stop
	this.wg.Wait()

	log.Info("all watchers stopped")
}

func (this *Monitor) ServeForever() {
	defer this.zkzone.Close()

	log.Info("starting...")

	signal.RegisterSignalsHandler(func(sig os.Signal) {
		log.Info("received signal: %s", strings.ToUpper(sig.String()))

		if this.leader {
			//this.candidate.Resign()
		}
		this.candidate.Stop()
		log.Info("election stopped, stopping watchers...")
		this.Stop()
		close(this.quit)
	}, syscall.SIGINT, syscall.SIGTERM)

	// start the api server
	apiServer := &http.Server{
		Addr:    this.apiAddr,
		Handler: this.router,
	}
	apiListener, err := net.Listen("tcp", this.apiAddr)
	if err == nil {
		go apiServer.Serve(apiListener)
	} else {
		log.Error("api http server: %v", err)
	}

	backend, err := zookeeper.New(this.zkzone.ZkAddrList(), &store.Config{})
	if err != nil {
		panic(err)
	}

	ip, _ := ctx.LocalIP()
	this.candidate = leadership.NewCandidate(backend, zk.KguardLeaderPath, ip.String(), 15*time.Second)
	electedCh, errCh := this.candidate.RunForElection()
	if err != nil {
		panic("Cannot run for election, store is probably down")
	}

	for {
		select {
		case isElected := <-electedCh:
			if isElected {
				log.Info("Won the election, starting all watchers")

				this.Start()
			} else {
				log.Warn("Fails the election, watching election events...")
				this.Stop()
			}

		case err := <-errCh:
			log.Error("Error during election: %v", err)

		case <-this.quit:
			log.Info("kguard bye!")
			log.Close()
			return
		}
	}

}

func (this *Monitor) ZkZone() *zk.ZkZone {
	return this.zkzone
}

func (this *Monitor) StopChan() <-chan struct{} {
	return this.stop
}

func (this *Monitor) WaitGroup() *sync.WaitGroup {
	return this.wg
}
