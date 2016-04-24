package main

import (
	"time"
)

type ReplicaManager struct {
	zkClient         *ZkClient
	kafkaScheduler   *KafkaScheduler
	logManager       *LogManager
	leaderPartitions map[Partition]struct{}

	replicaFetcherManager *ReplicaFetcherManager
}

func (this *ReplicaManager) Startup() {
	this.kafkaScheduler.Schedule("isr-expiration", this.maybeShrinkIsr, 0,
		config.getDuration("replica.lag.time.max.ms", time.Second*10))
}

func (this *ReplicaManager) maybeShrinkIsr() {

}

func (this *ReplicaManager) becomeLeaderOrFollower() {

}

func (this *ReplicaManager) makeFollowers() {

}

func (this *ReplicaManager) makeLeaders() {

}

func (this *ReplicaManager) checkpointHighWatermarks() {

}

func (this *ReplicaManager) recordFollowerPosition(topic string, partitionId int32, replicaId int, offset int64) {

}

func (this *ReplicaManager) stopReplicas() {

}

func (this *ReplicaManager) shutdown() {

}
