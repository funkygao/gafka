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
	monitor.RegisterWatcher("kafka.replica", func() monitor.Watcher {
		return &WatchReplicas{
			Tick: time.Minute,
		}
	})
}

// WatchReplicas reports kafka out of sync partitions num over time.
type WatchReplicas struct {
	Zkzone *zk.ZkZone
	Stop   chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchReplicas) Init(zkzone *zk.ZkZone, stop chan struct{}, wg *sync.WaitGroup) {
	this.Zkzone = zkzone
	this.Stop = stop
	this.Wg = wg
}

func (this *WatchReplicas) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	dead := metrics.NewRegisteredGauge("partitions.dead", nil)
	outOfSync := metrics.NewRegisteredGauge("partitions.outofsync", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("kafka.replica stopped")
			return

		case <-ticker.C:
			deadPartitions, outOfSyncPartitions := this.report()
			dead.Update(deadPartitions)
			outOfSync.Update(outOfSyncPartitions)
		}
	}

}

func (this *WatchReplicas) report() (deadPartitions, outOfSyncPartitions int64) {
	this.Zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		brokerList := zkcluster.BrokerList()
		if len(brokerList) == 0 {
			log.Warn("cluster[%s] empty brokers", zkcluster.Name())
			return
		}

		kfk, err := sarama.NewClient(brokerList, sarama.NewConfig())
		if err != nil {
			log.Error("cluster[%s] %v", zkcluster.Name(), err)
			return
		}
		defer kfk.Close()

		topics, err := kfk.Topics()
		if err != nil {
			log.Error("cluster[%s] %v", zkcluster.Name(), err)
			return
		}

		for _, topic := range topics {
			alivePartitions, err := kfk.WritablePartitions(topic)
			if err != nil {
				log.Error("cluster[%s] topic:%s %v", zkcluster.Name(), topic, err)
				continue
			}
			partions, err := kfk.Partitions(topic)
			if err != nil {
				log.Error("cluster[%s] topic:%s %v", zkcluster.Name(), topic, err)
				continue
			}

			// some partitions are dead
			if len(alivePartitions) != len(partions) {
				deadPartitions += 1
			}

			for _, partitionID := range alivePartitions {
				replicas, err := kfk.Replicas(topic, partitionID)
				if err != nil {
					log.Error("cluster[%s] topic:%s partition:%d %v",
						zkcluster.Name(), topic, partitionID, err)
					continue
				}

				isr, _, _ := zkcluster.Isr(topic, partitionID)
				if len(isr) != len(replicas) {
					outOfSyncPartitions += 1
				}
			}
		}
	})

	return
}
