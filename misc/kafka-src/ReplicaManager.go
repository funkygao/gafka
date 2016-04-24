package main

import (
	"time"
)

type ReplicaManager struct {
	zkClient       *ZkClient
	kafkaScheduler *KafkaScheduler
	logManager     *LogManager
}

func (this *ReplicaManager) Startup() {
	this.kafkaScheduler.Schedule("isr-expiration", this.maybeShrinkIsr, 0, config.getDuration("replica.lag.time.max.ms"))
}

func (this *ReplicaManager) maybeShrinkIsr() {

}
