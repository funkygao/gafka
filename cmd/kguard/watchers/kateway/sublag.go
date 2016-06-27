package kateway

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	"github.com/rcrowley/go-metrics"
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

	suspects map[string]struct{}
}

func (this *WatchSubLag) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.WaitGroup()
	this.suspects = make(map[string]struct{})
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

func (this *WatchSubLag) isSuspect(group string, topic string) bool {
	if _, present := this.suspects[group+"|"+topic]; present {
		return true
	}

	return false
}

func (this *WatchSubLag) suspect(group, topic string) {
	this.suspects[group+"|"+topic] = struct{}{}
}

func (this *WatchSubLag) unsuspect(group string, topic string) {
	delete(this.suspects, group+"|"+topic)
}

func (this *WatchSubLag) report() (lags int) {
	for group, consumers := range this.zkcluster.ConsumersByGroup("") {
		for _, c := range consumers {
			if !c.Online {
				continue
			}

			if c.ConsumerZnode == nil {
				log.Warn("group[%s] topic[%s/%s] unrecognized consumer", group, c.Topic, c.PartitionId)

				continue
			}

			if time.Since(c.ConsumerZnode.Uptime()) < time.Minute*2 {
				log.Info("group[%s] just started, topic[%s/%s]", group, c.Topic, c.PartitionId)

				this.unsuspect(group, c.Topic)
				continue
			}

			// offset commit every 1m, sublag runs every 1m, so the gap might be 2m
			// TODO lag too much, even if it's still alive, emit alarm
			elapsed := time.Since(c.Mtime.Time())
			if c.Lag == 0 || elapsed < time.Minute*3 {
				this.unsuspect(group, c.Topic)
				continue
			}

			// it might be lagging, but need confirm with last round
			if !this.isSuspect(group, c.Topic) {
				// suspect it, next round if it is still lagging, put on trial
				log.Warn("group[%s] suspected topic[%s/%s] %d - %d = %d, offset commit elapsed: %s",
					group, c.Topic, c.PartitionId, c.ProducerOffset, c.ConsumerOffset, c.Lag, elapsed.String())

				this.suspect(group, c.Topic)
				continue
			}

			// bingo! it IS lagging
			log.Warn("group[%s] confirmed topic[%s/%s] %d - %d = %d, offset commit elapsed: %s",
				group, c.Topic, c.PartitionId, c.ProducerOffset, c.ConsumerOffset, c.Lag, elapsed.String())

			lags++
		}
	}

	return
}
