package main

import (
	"flag"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/docker/leadership"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
	"github.com/julienschmidt/httprouter"
)

type Monitor struct {
	influxdbAddr   string
	influxdbDbName string
	apiAddr        string

	router *httprouter.Router
	zkzone *zk.ZkZone

	stop   chan struct{}
	leader bool

	executors []Executor
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
	this.router = httprouter.New()
	this.router.GET("/metrics", this.metricsHandler)

	if logFile == "stdout" {
		log.AddFilter("stdout", log.INFO, log.NewConsoleLogWriter())
	} else {
		log.DeleteFilter("stdout")

		filer := log.NewFileLogWriter(logFile, false)
		filer.SetFormat("[%d %T] [%L] (%S) %M")
		log.AddFilter("file", log.INFO, filer)
	}
}

func (this *Monitor) addExecutor(e Executor) {
	this.executors = append(this.executors, e)
}

func (this *Monitor) Stop() {
	close(this.stop)
}

func (this *Monitor) Start() {
	this.stop = make(chan struct{})

	go InfluxDB(ctx.Hostname(), metrics.DefaultRegistry, time.Minute,
		this.influxdbAddr, this.influxdbDbName, "", "", this.stop)

	this.executors = make([]Executor, 0)
	wg := new(sync.WaitGroup)
	this.addExecutor(&MonitorTopics{zkzone: this.zkzone, tick: time.Minute, stop: this.stop, wg: wg})
	this.addExecutor(&MonitorBrokers{zkzone: this.zkzone, tick: time.Minute, stop: this.stop, wg: wg})
	this.addExecutor(&MonitorReplicas{zkzone: this.zkzone, tick: time.Minute, stop: this.stop, wg: wg})
	this.addExecutor(&MonitorConsumers{zkzone: this.zkzone, tick: time.Minute, stop: this.stop, wg: wg})
	this.addExecutor(&MonitorClusters{zkzone: this.zkzone, tick: time.Minute, stop: this.stop, wg: wg})
	this.addExecutor(&MonitorF5{tick: time.Minute, stop: this.stop, wg: wg})
	for _, e := range this.executors {
		wg.Add(1)
		go e.Run()
	}

	log.Info("all executors ready")

	<-this.stop
	wg.Wait()

	log.Info("all executors stopped")
}

func (this *Monitor) ServeForever() {
	defer this.zkzone.Close()

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
	candidate := leadership.NewCandidate(backend, zk.KguardLeaderPath, ip.String(), 15*time.Second)
	electedCh, _, err := candidate.RunForElection()
	if err != nil {
		panic("Cannot run for election, store is probably down")
	}

	for isElected := range electedCh {
		if isElected {
			log.Info("Won the election, starting all executors")

			this.leader = true
			this.Start()
		} else {
			log.Warn("Lost the election, watching election events...")

			if this.leader {
				this.Stop()
			}
			this.leader = false
		}
	}

}
