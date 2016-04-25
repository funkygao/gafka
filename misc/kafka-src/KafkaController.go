package main

// will connect to each broker in the cluster
// watch /brokers/ids to know most recent topology
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

	// maintains 2 state machine
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

	this.replicaStateMachine.registerListeners() // watch /brokers/ids
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

// controlled shutdown
// 首先计算该broker lead的所有partitions
// 然后，把这些partitions移动到该partition ISR里的其他broker
func (this *KafkaController) shutdownBroker(brokerId int) {

}

func (this *KafkaController) onBrokerStartup(newBrokers []int) {

}

func (this *KafkaController) onBrokerFailure(deadBrokers []int) {

}

func (this *KafkaController) onNewTopicCreation(topics []string, newPartitions []TopicAndPartition) {

}
