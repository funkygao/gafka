package main

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
)

// MonitorConsumers monitors num of online consumer groups over the time.
type MonitorConsumers struct {
	zkzone *zk.ZkZone
	stop   chan struct{}
	tick   time.Duration
	wg     *sync.WaitGroup
}

func (this *MonitorConsumers) Init() {}

func (this *MonitorConsumers) Run() {
	defer this.wg.Done()

	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	consumerGroupsOnline := metrics.NewRegisteredGauge("consumer.groups.online", nil)
	consumerGroupsOffline := metrics.NewRegisteredGauge("consumer.groups.offline", nil)
	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			online, offline := this.report()
			consumerGroupsOffline.Update(offline)
			consumerGroupsOnline.Update(online)
		}
	}
}

func (this *MonitorConsumers) report() (online, offline int64) {
	this.zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		for _, cgInfo := range zkcluster.ConsumerGroups() {
			if len(cgInfo) > 0 {
				online += 1
			} else {
				offline += 1
			}
		}
	})
	return
}
