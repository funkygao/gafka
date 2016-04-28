package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
)

var _ Executor = &MonitorZk{}

type MonitorZk struct {
	zkzone *zk.ZkZone
	stop   chan struct{}
	tick   time.Duration
	wg     *sync.WaitGroup

	lastReceived int64
}

func (this *MonitorZk) Init() {

}

func (this *MonitorZk) Run() {
	defer this.wg.Done()

	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	qps := metrics.NewRegisteredGauge("zk.qps", nil)
	conns := metrics.NewRegisteredGauge("zk.conns", nil)
	znodes := metrics.NewRegisteredGauge("zk.znodes", nil)
	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			r, c, z := this.collectMetrics()
			if this.lastReceived > 0 {
				qps.Update((r - this.lastReceived) / int64(this.tick.Seconds()))
			}
			this.lastReceived = r

			conns.Update(c)
			znodes.Update(z)
		}
	}
}

func (this *MonitorZk) collectMetrics() (received, conns, znodes int64) {
	for _, statOutput := range this.zkzone.RunZkFourLetterCommand("stat") {
		stat := zk.ParseStatResult(statOutput)
		n, _ := strconv.Atoi(stat.Received)
		received += int64(n)
		n, _ = strconv.Atoi(stat.Connections)
		conns += int64(n)                // sum up the total connections
		n, _ = strconv.Atoi(stat.Znodes) // each node in zk should the same amount of znode
		znodes = int64(n)
	}

	return
}
