package main

import (
	"sync/atomic"
)

type KafkaServer struct {
	correlationId int64

	zkClient       *ZkClient
	kafkaScheduler *KafkaScheduler

	socketServer       *SocketServer
	requestHandlerPool *KafkaRequestHandlerPool
	kafkaHealthcheck   *KafkaHealthcheck
	replicaManager     *ReplicaManager
	logManager         *LogManager
	topicConfigManager *TopicConfigManager
	apis               *KafkaApis
	kafkaController    *KafkaController
}

func (this *KafkaServer) Startup() {
	this.kafkaScheduler.Startup()

	this.zkClient = initZk()

	this.logManager.Startup()
	this.socketServer.Startup()
	this.replicaManager.Startup()
	this.kafkaController.Startup()
	this.topicConfigManager.Startup()
	this.kafkaHealthcheck.Startup()

}

func (this *KafkaServer) controlledShutdown() {
	controllerId := 1 // get controller id from zk
	// issue a controlled shutdown to the controller
	atomic.AddInt64(&this.correlationId, 1)
	request := ControlledShutdownRequest{
		this.correlationId, myBrokerId}
}
