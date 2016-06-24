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
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	zkcluster *zk.ZkCluster
}

func (this *WatchSubLag) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.WaitGroup()
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
			// TODO lag too much, even if it's still alive, emit alarm
			elapsed := time.Since(c.Mtime.Time())
			if c.Lag > 0 && elapsed >= time.Minute*3 {
				// case:
				//   consumer A started 20h ago, last commit 10m ago, then no message arrives
				//   now, 1 new message arrives, and WatchSubLag is awaken: false alarm
				log.Warn("group[%s] topic[%s/%s] %d - %d = %d, elapsed: %s",
					group, c.Topic, c.PartitionId,
					c.ProducerOffset, c.ConsumerOffset, c.Lag, elapsed.String())

				if c.ConsumerZnode == nil {
					log.Warn("group[%s] topic[%s/%s] unrecognized consumer", group,
						c.Topic, c.PartitionId)

					lags++
				} else {
					if time.Since(c.ConsumerZnode.Uptime()) > time.Minute*3 {
						lags++
					} else {
						// the consumer just started
					}
				}
			}
		}
	}

	return
}
