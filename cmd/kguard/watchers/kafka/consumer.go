package kafka

import (
	"strings"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/structs"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kafka.consumer", func() monitor.Watcher {
		return &WatchConsumers{
			Tick: time.Minute,
		}
	})
}

// WatchConsumers monitors num of kafka online consumer groups over the time.
type WatchConsumers struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	logFrequentConsumer bool

	offsetMtimeMap map[structs.GroupTopicPartition]time.Time

	consumerQps map[string]metrics.Meter
	lastOffsets map[string]int64
}

func (this *WatchConsumers) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
	this.logFrequentConsumer = false
	this.offsetMtimeMap = make(map[structs.GroupTopicPartition]time.Time, 100)
}

func (this *WatchConsumers) Set(key string) {
	switch key {
	case "frequent-log-on":
		this.logFrequentConsumer = true

	case "frequent-log-off":
		this.logFrequentConsumer = false
	}
}

func (this *WatchConsumers) Run() {
	defer this.Wg.Done()

	this.consumerQps = make(map[string]metrics.Meter, 10)
	this.lastOffsets = make(map[string]int64, 10)

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	frequentCommitTick := time.NewTicker(time.Second * 30)
	defer frequentCommitTick.Stop()

	consumerGroupsOnline := metrics.NewRegisteredGauge("consumer.groups.online", nil)
	consumerGroupsOffline := metrics.NewRegisteredGauge("consumer.groups.offline", nil)
	tooFrequentOffsetCommit := metrics.NewRegisteredGauge("consumer.frequent.offset.commit", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("kafka.consumer stopped")
			return

		case <-ticker.C:
			online, offline := this.report()
			consumerGroupsOffline.Update(offline)
			consumerGroupsOnline.Update(online)

			this.runSubQpsTimer()

		case <-frequentCommitTick.C:
			tooFrequentOffsetCommit.Update(this.frequentOffsetCommit())
		}
	}
}

func (this *WatchConsumers) report() (online, offline int64) {
	this.Zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		for _, cgInfo := range zkcluster.ConsumerGroups() {
			if len(cgInfo) > 0 {
				online++
			} else {
				offline++
			}
		}
	})

	return
}

func (this *WatchConsumers) frequentOffsetCommit() (n int64) {
	const frequentThreshold = time.Second * 10

	this.Zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		for group, consumers := range zkcluster.ConsumersByGroup("") {
			for _, c := range consumers {
				if !c.Online {
					continue
				}

				if c.ConsumerZnode == nil {
					log.Warn("cluster[%s] group[%s] topic[%s/%s] unrecognized consumer", zkcluster.Name(), group, c.Topic, c.PartitionId)

					continue
				}

				gtp := structs.GroupTopicPartition{Group: group, Topic: c.Topic, PartitionID: c.PartitionId}
				if t, present := this.offsetMtimeMap[gtp]; present {
					if interval := c.Mtime.Time().Sub(t); interval < frequentThreshold {
						if this.logFrequentConsumer {
							log.Warn("cluster[%s] group[%s] topic[%s/%s] too frequent offset commit: %s", zkcluster.Name(), group, c.Topic, c.PartitionId, interval)
						}

						n++
					}
				}

				this.offsetMtimeMap[gtp] = c.Mtime.Time()
			}
		}
	})

	return
}

func (this *WatchConsumers) runSubQpsTimer() {
	this.Zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		consumerGroups := zkcluster.ConsumerGroups()
		for group := range consumerGroups {
			offsetMap := zkcluster.ConsumerOffsetsOfGroup(group)
			for topic, m := range offsetMap {
				offsetOfGroupOnTopic := int64(0)
				for _, offset := range m {
					offsetOfGroupOnTopic += offset
				}

				// cluster, topic, group, offset
				tag := telemetry.Tag(zkcluster.Name(), strings.Replace(topic, ".", "_", -1), strings.Replace(group, ".", "_", -1))
				if _, present := this.consumerQps[tag]; !present {
					this.consumerQps[tag] = metrics.NewRegisteredMeter(tag+"consumer.qps", nil)
				}
				lastOffset := this.lastOffsets[tag]
				if lastOffset == 0 {
					// first run
					this.lastOffsets[tag] = offsetOfGroupOnTopic
				} else {
					delta := offsetOfGroupOnTopic - lastOffset
					if delta >= 0 {
						this.consumerQps[tag].Mark(delta)
						this.lastOffsets[tag] = offsetOfGroupOnTopic
					} else {
						log.Warn("cluster[%s] topic[%s] group[%s] offset rewinds: %d %d",
							zkcluster.Name(), topic, group, offsetOfGroupOnTopic, lastOffset)
					}

				}
			}
		}
	})
}
