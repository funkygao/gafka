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

	// connect to zk and setup common paths
	this.zkClient = initZk()

	this.logManager.createAndValidateLogDirs()
	this.logManager.loadLogs()
	this.logManager.Startup() // schedule

	this.socketServer.Startup()
	this.replicaManager.Startup()
	this.kafkaController.Startup()
	this.topicConfigManager.Startup()
	this.kafkaHealthcheck.Startup()

}

func (this *KafkaServer) controlledShutdown() {
	if !config.getBool("controlled.shutdown.enable", false) {
		return
	}

	controllerId := 1 // get controller id from zk
	// issue a controlled shutdown to the controller
	atomic.AddInt64(&this.correlationId, 1)

	for retry := 0; retry < 3; retry++ {
		conrollerId := this.zkClient.getController()
		request := ControlledShutdownRequest{
			this.correlationId, myBrokerId}
		sendRequestAndRecvResponse(controllerId, request)
		// will trigger KafkaApis.handleControlledShutdownRequest
	}

}
