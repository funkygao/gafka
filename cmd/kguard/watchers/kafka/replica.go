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
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchReplicas) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchReplicas) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	dead := metrics.NewRegisteredGauge("partitions.dead", nil)
	outOfSync := metrics.NewRegisteredGauge("partitions.outofsync", nil)
	badReplica := metrics.NewRegisteredGauge("partitions.badreplica", nil)

	for {
		select {
		case <-this.Stop:
			log.Info("kafka.replica stopped")
			return

		case <-ticker.C:
			deadPartitions, outOfSyncPartitions, failReplicas := this.report()
			dead.Update(deadPartitions)
			outOfSync.Update(outOfSyncPartitions)
			badReplica.Update(failReplicas)
		}
	}

}

func (this *WatchReplicas) report() (deadPartitions, outOfSyncPartitions, failReplicas int64) {
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
				deadPartitions++
			}

			for _, partitionID := range alivePartitions {
				replicas, err := kfk.Replicas(topic, partitionID)
				if err != nil {
					log.Error("cluster[%s] topic:%s/%d %v", zkcluster.Name(), topic, partitionID, err)
					failReplicas++
					continue
				}

				isr, _, _ := zkcluster.Isr(topic, partitionID)
				if len(isr) != len(replicas) {
					outOfSyncPartitions++
				}
			}
		}
	})

	return
}
