package kateway

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kateway.sublag", func() monitor.Watcher {
		return &WatchSubLag{
			Tick: time.Minute,
		}
	})
}

// SubLag monitors aliveness of kateway cluster.
type WatchSubLag struct {
	Zkzone *zk.ZkZone
	Stop   chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	zkcluster *zk.ZkCluster
}

func (this *WatchSubLag) Init(zkzone *zk.ZkZone, stop chan struct{}, wg *sync.WaitGroup) {
	this.Zkzone = zkzone
	this.Stop = stop
	this.Wg = wg
}

func (this *WatchSubLag) Run() {
	defer this.Wg.Done()

	this.zkcluster = this.Zkzone.NewCluster("bigtopic") // TODO

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	subLagGroups := metrics.NewRegisteredGauge("sub.lags", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("kateway.sublag stopped")
			return

		case <-ticker.C:
			subLagGroups.Update(int64(this.report()))

		}
	}
}

func (this *WatchSubLag) report() (lags int) {
	for group, consumers := range this.zkcluster.ConsumersByGroup("") {
		for _, c := range consumers {
			if !c.Online {
				continue
			}

			// offset commit every 1m, sublag runs every 1m, so the gap might be 2m
			elapsed := time.Since(c.Mtime.Time())
			if c.Lag > 0 && elapsed >= time.Minute*3 {
				log.Warn("group[%s] topic[%s/%s] %d - %d = %d, elapsed: %s %s", group, c.Topic, c.PartitionId,
					c.ProducerOffset, c.ConsumerOffset, c.Lag, elapsed.String(), c.Mtime.Time())

				lags++
			}
		}
	}

	return
}
