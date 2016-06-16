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
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	"github.com/funkygao/golib/signal"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

type Monitor struct {
	influxdbAddr   string
	influxdbDbName string
	apiAddr        string

	router *httprouter.Router
	zkzone *zk.ZkZone

	candidate *leadership.Candidate

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

		filer := log.NewFileLogWriter(logFile, true, false, 0)
		filer.SetFormat("[%d %T] [%L] (%S) %M")
		log.AddFilter("file", log.INFO, filer)
	}
}

func (this *Monitor) Stop() {
	if this.leader {
		close(this.stop)
	}
	this.leader = false
}

func (this *Monitor) Start() {
	this.leader = true

	this.stop = make(chan struct{})

	go InfluxDB(ctx.Hostname(), metrics.DefaultRegistry, time.Minute,
		this.influxdbAddr, this.influxdbDbName, "", "", this.stop)

	wg := new(sync.WaitGroup)
	for name, watcherFactory := range registeredWatchers {
		watcher := watcherFactory()
		watcher.Init(this.zkzone, this.stop, wg)

		log.Info("created and starting watcher: %s", name)

		wg.Add(1)
		go watcher.Run()
	}

	log.Info("all watchers ready!")

	<-this.stop
	wg.Wait()

	log.Info("all watchers stopped")
}

func (this *Monitor) ServeForever() {
	defer this.zkzone.Close()

	signal.RegisterSignalsHandler(func(sig os.Signal) {
		log.Info("received signal: %s", strings.ToUpper(sig.String()))

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
