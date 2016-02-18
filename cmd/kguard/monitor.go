package main

import (
	"flag"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
)

type Monitor struct {
	zone           string
	influxdbAddr   string
	influxdbDbName string

	topic   *MonitorTopics
	broker  *MonitorBrokers
	replica *MonitorReplicas
}

func (this *Monitor) Init() {
	flag.StringVar(&this.zone, "z", "", "zone")
	flag.StringVar(&this.influxdbAddr, "influxAddr", "", "influxdb addr")
	flag.StringVar(&this.influxdbDbName, "db", "", "influxdb db name")
	flag.Parse()

	if this.zone == "" || this.influxdbDbName == "" || this.influxdbAddr == "" {
		panic("run help ")
	}

}

func (this *Monitor) ServeForever() {
	ctx.LoadFromHome()

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	this.topic = &MonitorTopics{zkzone: zkzone}
	this.broker = &MonitorBrokers{zkzone: zkzone}
	this.replica = &MonitorReplicas{zkzone: zkzone}

	go this.topic.Run()
	select {}

}
