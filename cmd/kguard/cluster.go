package main

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
)

var _ Executor = &MonitorClusters{}

// MonitorClusters montor num of clusters over the time.
type MonitorClusters struct {
	zkzone *zk.ZkZone
	stop   chan struct{}
	tick   time.Duration
	wg     *sync.WaitGroup
}

func (this *MonitorClusters) Init() {}

func (this *MonitorClusters) Run() {
	defer this.wg.Done()

	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	clusters := metrics.NewRegisteredGauge("clusters", nil)
	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			clusters.Update(int64(len(this.zkzone.Clusters())))
		}
	}

}
