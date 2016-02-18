package main

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

type MonitorTopics struct {
	zkzone *zk.ZkZone
}

func (this *MonitorTopics) Run() {
	tick := time.NewTicker(time.Hour)
	for {
		select {
		case <-tick.C:

		}

	}

}

func (this *MonitorTopics) totalOffsets() (total int64) {
	this.zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		kfk, err := sarama.NewClient(zkcluster.BrokerList(), sarama.NewConfig())
		defer kfk.Close()

		topics, err := kfk.Topics()
		if err != nil {
			log.Error(err)
			return
		}

		for _, topic := range topics {
			partions, err := kfk.Partitions(topic)
			if err != nil {
				log.Error(err)
				continue
			}

			for _, partitionId := range partions {
				latestOffset, err := kfk.GetOffset(topic, partitionId,
					sarama.OffsetNewest)
				if err != nil {
					log.Error(err)
					continue
				}

				total += latestOffset
			}
		}

	})

	return
}
