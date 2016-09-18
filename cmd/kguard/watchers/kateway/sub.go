package kateway

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/structs"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kateway.sub", func() monitor.Watcher {
		return &WatchSub{
			Tick: time.Minute,
		}
	})
}

type subStatus struct {
	ProducedOffset, ConsumedOffset int64
	Time                           time.Time
}

// WatchSub monitors Sub status of kateway cluster.
type WatchSub struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	zkclusters []*zk.ZkCluster

	suspects map[structs.GroupTopicPartition]subStatus
}

func (this *WatchSub) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
	this.suspects = make(map[structs.GroupTopicPartition]subStatus)
}

func (this *WatchSub) Run() {
	defer this.Wg.Done()

	this.zkclusters = this.Zkzone.PublicClusters() // TODO sync with clusters change

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	subLagGroups := metrics.NewRegisteredGauge("sub.lags", nil)
	subConflictGroup := metrics.NewRegisteredGauge("sub.conflict", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("kateway.sub stopped")
			return

		case <-ticker.C:
			lags := this.subLags()
			conflictGroups := this.subConflicts()
			subLagGroups.Update(int64(lags))
			subConflictGroup.Update(int64(conflictGroups))

		}
	}
}

func (this *WatchSub) isSuspect(group, topic string, partitionID string) bool {
	if _, present := this.suspects[structs.GroupTopicPartition{Group: group, Topic: topic, PartitionID: partitionID}]; present {
		return true
	}

	return false
}

func (this *WatchSub) suspect(group, topic string, partitionID string, producedOffset, consumedOffset int64, t time.Time) {
	this.suspects[structs.GroupTopicPartition{Group: group, Topic: topic, PartitionID: partitionID}] = subStatus{
		ProducedOffset: producedOffset,
		ConsumedOffset: consumedOffset,
		Time:           t,
	}
}

func (this *WatchSub) unsuspect(group, topic string, partitionID string) {
	delete(this.suspects, structs.GroupTopicPartition{Group: group, Topic: topic, PartitionID: partitionID})
}

func (this *WatchSub) isCriminal(group, topic string, partitionID string, producedOffset, consumedOffset int64, t time.Time) bool {
	lastStat, ok := this.suspects[structs.GroupTopicPartition{Group: group, Topic: topic, PartitionID: partitionID}]
	if !ok {
		// should never happen
		log.Error("group[%s] topic[%s/%s] should never happen!", group, topic, partitionID)
		return false
	}

	if lastStat.ProducedOffset < producedOffset && lastStat.ConsumedOffset >= consumedOffset {
		// new messages produced during this period but the consumer didn't move ahead
		return true
	}

	return false
}

func (this *WatchSub) subLags() (lags int) {
	now := time.Now()
	// find sub lags
	for _, zkcluster := range this.zkclusters {
		for group, consumers := range zkcluster.ConsumersByGroup("") {
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

					this.unsuspect(group, c.Topic, c.PartitionId)
					continue
				}

				// offset commit every 1m, sublag runs every 1m, so the gap might be 2m
				// TODO lag too much, even if it's still alive, emit alarm
				elapsed := time.Since(c.Mtime.Time())
				if c.Lag == 0 || elapsed < time.Minute*3 {
					this.unsuspect(group, c.Topic, c.PartitionId)
					continue
				}

				// it might be lagging, but need confirm with last round
				if !this.isSuspect(group, c.Topic, c.PartitionId) {
					// suspect it, next round if it is still lagging, put on trial
					log.Warn("group[%s] suspected topic[%s/%s] %d - %d = %d, offset commit elapsed: %s",
						group, c.Topic, c.PartitionId, c.ProducerOffset, c.ConsumerOffset, c.Lag, elapsed.String())

					this.suspect(group, c.Topic, c.PartitionId, c.ProducerOffset, c.ConsumerOffset, now)
					continue
				}

				if this.isCriminal(group, c.Topic, c.PartitionId, c.ProducerOffset, c.ConsumerOffset, now) {
					// bingo! consumer is lagging and seems to be DEAD
					log.Error("group[%s] confirmed topic[%s/%s] %d - %d = %d, offset commit elapsed: %s",
						group, c.Topic, c.PartitionId, c.ProducerOffset, c.ConsumerOffset, c.Lag, elapsed.String())

					lags++
				} else {
					log.Warn("group[%s] lagging but still alive topic[%s/%s] %d - %d = %d, offset commit elapsed: %s",
						group, c.Topic, c.PartitionId, c.ProducerOffset, c.ConsumerOffset, c.Lag, elapsed.String())
				}

			}
		}

	}

	return
}

func (this *WatchSub) subConflicts() (conflictGroups int) {
	// find sub conflicts
	for _, zkcluster := range this.zkclusters {
		groupTopicsMap := make(map[string]map[string]struct{}) // group:sub topics

		for group, consumers := range zkcluster.ConsumerGroups() {
			if len(consumers) == 0 {
				continue
			}

			for _, c := range consumers {
				for topic, _ := range c.Subscription {
					if len(groupTopicsMap[group]) == 0 {
						groupTopicsMap[group] = make(map[string]struct{}, 5)
					}
					groupTopicsMap[group][topic] = struct{}{}
				}
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
			for t := range topics {
				topicsLabel = append(topicsLabel, t)
			}
			sort.Strings(topicsLabel)

			log.Warn("group[%s] consuming more than 1 topics: %s", group, strings.Join(topicsLabel, ", "))
		}
	}

	return
}
