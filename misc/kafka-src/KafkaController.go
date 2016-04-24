package main

type KafkaController struct {
	epoch         int
	correlationId int64
	zkclient      *ZkClient

	deleteTopicManager *DeleteTopicManager

	allTopics                                    []string
	partitionReplicaAssignment                   map[TopicAndPartition]int64
	partitionLeadershipInfo                      map[TopicAndPartition]LeaderIsrAndControllerEpoch
	partitionsBeingReassigned                    map[TopicAndPartition]ReassignedPartitionsContext
	partitionsUndergoingPreferredReplicaElection []TopicAndPartition

	partitionStateMachine PartitionStateMachine
	replicaStateMachine   ReplicaStateMachine

	controllerElector ZookeeperLeaderElector
	controllerContext ControllerContext
}

func (this *KafkaController) Startup() {
	registerControllerChangedListener()

	this.zkclient.subscribeStateChanges(func() {

	})

	this.controllerElector = ZookeeperLeaderElector{"/controller", this.onBecomingLeader,
		this.onControllerResignation, config.brokerId}
	this.controllerElector.Startup()

}

func (this *KafkaController) onBecomingLeader() {
	incrementControllerEpoch(this.zkclient)
	registerReassignedPartitionsListener()

	this.replicaStateMachine.registerListeners()
	this.partitionStateMachine.registerListeners()

	this.initializeControllerContext()

	this.replicaStateMachine.Startup()
	this.partitionStateMachine.Startup()

	for _, topic := range this.controllerContext.allTopics {
		this.partitionStateMachine.registerPartitionChangeListener(topic)
	}

	maybeTriggerPartitionReassignment()

	/* send partition leadership info to all live brokers */
	sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds)

	this.deleteTopicManager.Startup()
}

func (this *KafkaController) onControllerResignation() {
	if this.deleteTopicManager != nil {
		this.deleteTopicManager.shutdown()
	}

	this.partitionStateMachine.shutdown()
	this.replicaStateMachine.shutdown()
}

func (this *KafkaController) initializeControllerContext() {
	this.controllerContext = ControllerContext{}
	this.controllerContext.liveBrokers = this.zkclient.getAllBrokersInCluster()
	this.controllerContext.allTopics = this.zkclient.getAllTopics()
	this.controllerContext.partitionReplicaAssignment = this.zkclient.getReplicaAssignmentForTopics(this.controllerContext.allTopics)
}

func (this *KafkaController) onNewPartitionCreation(newPartitions []TopicAndPartition) {

}

func (this *KafkaController) sessionExpirationListener() {
	this.onControllerResignation()
}

func (this *KafkaController) shutdownBroker(brokerId int) {

}
