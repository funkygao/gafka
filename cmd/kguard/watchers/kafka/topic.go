package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/cmd/kguard/watchers"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

var _ watchers.Watcher = &WatchTopics{}

// WatchTopics montor kafka total msg count over time.
type WatchTopics struct {
	Zkzone *zk.ZkZone
	Stop   chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchTopics) Init() {}

func (this *WatchTopics) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	pubQps := metrics.NewRegisteredMeter("pub.qps", nil)
	offsets := metrics.NewRegisteredGauge("msg.cum", nil)
	topics := metrics.NewRegisteredGauge("topics", nil)
	partitions := metrics.NewRegisteredGauge("partitions", nil)
	brokers := metrics.NewRegisteredGauge("brokers", nil)
	var lastTotalOffsets int64
	for {

		select {
		case <-this.Stop:
			return

		case <-ticker.C:
			o, t, p, b := this.report()
			offsets.Update(o)
			topics.Update(t)
			partitions.Update(p)
			brokers.Update(b)

			if lastTotalOffsets > 0 {
				if o-lastTotalOffsets > 0 {
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

			topicsN += 1

			for _, partitionId := range partions {
				latestOffset, err := kfk.GetOffset(topic, partitionId,
					sarama.OffsetNewest)
				if err != nil {
					log.Error("cluster[%s] topic:%s partition:%d %v",
						zkcluster.Name(), topic, partitionId, err)
					continue
				}

				partitionN += 1
				totalOffsets += latestOffset
			}
		}

	})

	return
}
