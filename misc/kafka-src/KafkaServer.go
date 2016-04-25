package main

import (
	"log"
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
	this.logManager.Startup() // schedule retention,etc

	this.socketServer.Startup() // read to accept new conn

	this.apis = KafkaApis{}
	// request handler will dispatch request to KafkaApis
	this.requestHandlerPool = KafkaRequestHandlerPool{
		brokerId: config.brokerId,
		sock:     this.socketServer,
	}

	this.replicaManager.Startup()
	this.kafkaController.Startup()

	this.topicConfigManager.Startup()

	// tell everyone we are alive: /brokers/ids/{brokerId}
	// only controller watch /brokers/ids children changes
	this.kafkaHealthcheck.Startup()

	log.Println("started")
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
