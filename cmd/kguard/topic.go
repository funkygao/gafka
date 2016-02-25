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

	c := metrics.NewRegisteredGauge("msg.cum", metrics.DefaultRegistry)
	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			total := this.totalOffsets()
			log.Info("zone[%s] total offsets: %d", this.zkzone.Name(), total)
			c.Update(total)
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
