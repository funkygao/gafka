package main

import (
	"time"

	"github.com/funkygao/gafka/zk"
)

// MonitorReplicas reports out of sync partitions num over time.
type MonitorReplicas struct {
	zkzone *zk.ZkZone
	stop   chan struct{}
	tick   time.Duration
}

func (this *MonitorReplicas) Run() {
	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:

		}
	}

}
