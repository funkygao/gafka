package main

import (
	"log"
	"os"
	"time"

	"github.com/funkygao/metrics"
)

var (
	stats *exchangeStats
)

type exchangeStats struct {
	MsgPerSecond metrics.Meter
}

func newExchangeStats() *exchangeStats {
	this := &exchangeStats{
		MsgPerSecond: metrics.NewMeter(),
	}

	metrics.Register("msg.per.second", this.MsgPerSecond)
	return this
}

func (this *exchangeStats) start() {
	go metrics.Log(metrics.DefaultRegistry, time.Second*10,
		log.New(os.Stdout, "metrics: ", log.Lmicroseconds))
}
