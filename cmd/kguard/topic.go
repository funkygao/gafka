package main

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

// MonitorTopics montor total msg count over time.
type MonitorTopics struct {
	zkzone *zk.ZkZone
	stop   chan struct{}
	tick   time.Duration
	wg     *sync.WaitGroup
}

func (this *MonitorTopics) Run() {
	defer this.wg.Done()

	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	pubQps := metrics.NewRegisteredMeter("pub.qps", nil)
	offsets := metrics.NewRegisteredGauge("msg.cum", nil)
	topics := metrics.NewRegisteredGauge("topics", nil)
	partitions := metrics.NewRegisteredGauge("partitions", nil)
	brokers := metrics.NewRegisteredGauge("brokers", nil)
	var lastTotalOffsets int64
	for {

		select {
		case <-this.stop:
			return

		case <-ticker.C:
			o, t, p, b := this.report()
			offsets.Update(o)
			topics.Update(t)
			partitions.Update(p)
			brokers.Update(b)

			if lastTotalOffsets > 0 {
				pubQps.Mark(o - lastTotalOffsets)
			}
			lastTotalOffsets = o
		}
	}

}

func (this *MonitorTopics) report() (totalOffsets int64, topicsN int64,
	partitionN int64, brokersN int64) {
	this.zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
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
