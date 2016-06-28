package kateway

import (
	"sort"
	"strings"
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

	zkclusters []*zk.ZkCluster

	suspects map[string]struct{}
}

func (this *WatchSubLag) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
	this.suspects = make(map[string]struct{})
}

func (this *WatchSubLag) Run() {
	defer this.Wg.Done()

	this.zkclusters = this.Zkzone.PublicClusters() // TODO sync with clusters change

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	subLagGroups := metrics.NewRegisteredGauge("sub.lags", nil)
	subConflictGroup := metrics.NewRegisteredGauge("sub.conflict", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("kateway.sublag stopped")
			return

		case <-ticker.C:
			lags, conflictGroups := this.report()
			subLagGroups.Update(int64(lags))
			subConflictGroup.Update(int64(conflictGroups))

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

func (this *WatchSubLag) report() (lags, conflictGroups int) {
	for _, zkcluster := range this.zkclusters {
		groupTopicsMap := make(map[string]map[string]struct{}) // group:sub topics

		for group, consumers := range zkcluster.ConsumersByGroup("") {
			for _, c := range consumers {
				if !c.Online {
					continue
				}

				if c.ConsumerZnode == nil {
					log.Warn("group[%s] topic[%s/%s] unrecognized consumer", group, c.Topic, c.PartitionId)

					continue
				}

				// record each group is consuming what topics
				for topic, _ := range c.ConsumerZnode.Subscription {
					if _, present := groupTopicsMap[group]; !present {
						groupTopicsMap[group] = make(map[string]struct{}, 5)
					}
					groupTopicsMap[group][topic] = struct{}{}
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

		// Sub disallow the same group to sub multiple topics
		for group, topics := range groupTopicsMap {
			if len(topics) <= 1 {
				continue
			}

			// conflict found!
			conflictGroups++

			// the same consumer group is consuming more than 1 topics
			topicsLabel := make([]string, 0, len(topics))
			for t, _ := range topics {
				topicsLabel = append(topicsLabel, t)
			}
			sort.Strings(topicsLabel)

			log.Warn("group[%s] consuming more than 1 topics: %s", group, strings.Join(topicsLabel, ","))
		}
	}

	return
}
