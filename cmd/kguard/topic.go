package main

import (
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
}

func (this *MonitorTopics) Run() {
	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	offsets := metrics.NewRegisteredGauge("msg.cum", nil)
	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			offsets.Update(this.totalOffsets())
		}
	}

}

func (this *MonitorTopics) totalOffsets() (total int64) {
	this.zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
		defer kfk.Close()

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

			for _, partitionId := range partions {
				latestOffset, err := kfk.GetOffset(topic, partitionId,
					sarama.OffsetNewest)
				if err != nil {
					log.Error("cluster[%s] topic:%s partition:%d %v",
						zkcluster.Name(), topic, partitionId, err)
					continue
				}

				total += latestOffset
			}
		}

	})

	return
}
