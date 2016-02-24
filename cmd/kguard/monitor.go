package main

import (
	"flag"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
)

type Monitor struct {
	zone           string
	influxdbAddr   string
	influxdbDbName string

	stop chan struct{}

	executors []Executor
}

func (this *Monitor) Init() {
	flag.StringVar(&this.zone, "z", "", "zone, required")
	flag.StringVar(&this.influxdbAddr, "influxAddr", "", "influxdb addr, required")
	flag.StringVar(&this.influxdbDbName, "db", "", "influxdb db name, required")
	flag.Parse()

	if this.zone == "" || this.influxdbDbName == "" || this.influxdbAddr == "" {
		panic("run help ")
	}

	this.executors = make([]Executor, 0)
}

func (this *Monitor) addExecutor(e Executor) {
	this.executors = append(this.executors, e)
}

func (this *Monitor) Stop() {
	close(this.stop)
}

func (this *Monitor) ServeForever() {
	ctx.LoadFromHome()

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))

	this.addExecutor(&MonitorTopics{zkzone: zkzone, tick: time.Minute, stop: this.stop})
	this.addExecutor(&MonitorBrokers{zkzone: zkzone, tick: time.Minute, stop: this.stop})
	this.addExecutor(&MonitorReplicas{zkzone: zkzone, tick: time.Minute, stop: this.stop})

	for _, e := range this.executors {
		go e.Run()
	}

	select {}

}
