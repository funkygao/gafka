package main

import (
	"time"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
)

// MonitorClusters montor num of clusters over the time.
type MonitorClusters struct {
	zkzone *zk.ZkZone
	stop   chan struct{}
	tick   time.Duration
}

func (this *MonitorClusters) Run() {
	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	clusters := metrics.NewRegisteredGauge("clusters.num", nil)
	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			clusters.Update(int64(len(this.zkzone.Clusters())))
		}
	}

}
