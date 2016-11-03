package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kafka.topic", func() monitor.Watcher {
		return &WatchTopics{
			Tick: time.Minute,
		}
	})
}

// WatchTopics montor kafka total msg count over time.
type WatchTopics struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchTopics) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchTopics) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	pubQps := metrics.NewRegisteredMeter("pub.qps", nil) // TODO add tag cluster/topic
	offsets := metrics.NewRegisteredGauge("msg.cum", nil)
	topics := metrics.NewRegisteredGauge("topics", nil)
	partitions := metrics.NewRegisteredGauge("partitions", nil)
	brokers := metrics.NewRegisteredGauge("brokers", nil)
	newTopics := metrics.NewRegisteredGauge("kfk.newtopic.1d", nil)
	var lastTotalOffsets int64
	for {

		select {
		case <-this.Stop:
			log.Info("kafka.topic stopped")
			return

		case now := <-ticker.C:
			o, t, p, b := this.report()
			offsets.Update(o)
			topics.Update(t)
			partitions.Update(p)
			brokers.Update(b)
			newTopics.Update(this.newTopicsSince(now, time.Hour*24))

			if lastTotalOffsets > 0 {
				if o-lastTotalOffsets >= 0 {
					pubQps.Mark(o - lastTotalOffsets)
					lastTotalOffsets = o
				} else {
					// e,g. some topics are dead, so the next total offset < lastTotalOffset
					// in this case, we skip this offset metric: only log warning
					log.Warn("offset backwards: %d %d", o, lastTotalOffsets)
				}
			} else {
				// the 1st run inside the loop
				lastTotalOffsets = o
			}

		}
	}

}

func (this *WatchTopics) newTopicsSince(now time.Time, since time.Duration) (n int64) {
	excludedClusters := this.Zkzone.PublicClusters()
	this.Zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		// kateway topics excluded
		clusterExcluded := false
		for _, cluster := range excludedClusters {
			if cluster.Name() == zkcluster.Name() {
				clusterExcluded = true
				break
			}
		}
		if clusterExcluded {
			return
		}

		// find recently how many topics created
		for _, ctime := range zkcluster.TopicsCtime() {
			if now.Sub(ctime) <= since {
				n += 1
			}
		}
	})

	return
}

func (this *WatchTopics) report() (totalOffsets int64, topicsN int64,
	partitionN int64, brokersN int64) {
	this.Zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		brokerList := zkcluster.BrokerList()
		kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
		if err != nil {
			log.Error("cluster[%s] %v", zkcluster.Name(), err)
			return
		}
		defer kfk.Close()

		brokersN += int64(len(brokerList))

		topics, err := kfk.Topics()
		if err != nil {
			log.Error("cluster[%s] %v", zkcluster.Name(), err)
			return
		}

		for _, topic := range topics {
			partions, err := kfk.Partitions(topic)
			if err != nil {
				log.Error("cluster[%s] topic:%s %v", zkcluster.Name(), topic, err)
				continue
			}

			topicsN++

			for _, partitionId := range partions {
				latestOffset, err := kfk.GetOffset(topic, partitionId,
					sarama.OffsetNewest)
				if err != nil {
					log.Error("cluster[%s] topic[%s/%d]: %v",
						zkcluster.Name(), topic, partitionId, err)
					continue
				}

				partitionN++
				totalOffsets += latestOffset
			}
		}

	})

	return
}
