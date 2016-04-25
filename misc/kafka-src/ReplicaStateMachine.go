package main

type ReplicaStateMachine struct {
	zkClient   *ZkClient
	controller *KafkaController

	replicaState map[PartitionAndReplica]ReplicaState
}

func (this *ReplicaStateMachine) registerListeners() {
	this.zkClient.subscribeChildChanges("/brokers/ids", func() {
		// 重新计算哪个broker是新增的，哪个是死亡的
		var newBrokerIds, deadBrokerIds []int

		// 让controller connect to new brokers/disconnect from dead brokers

		//
		this.controller.onBrokerStartup(newBrokerIds)
		this.controller.onBrokerFailure(deadBrokerIds)
	})
}

func (this *ReplicaStateMachine) Startup() {
	for topicPartition, assignedReplicas := range this.controller.controllerContext.partitionReplicaAssignment {
		for _, replicaId := range assignedReplicas {
			partitionAndReplica := PartitionAndReplica{
				Topic:       topicPartition.Topic,
				PartitionId: topicPartition.PartitionId,
				Replica:     replicaId,
			}
			if this.controller.controllerContext.liveBrokers.Contains(replicaId) {
				this.replicaState[partitionAndReplica] = OnlineReplica
			} else {
				this.replicaState[partitionAndReplica] = ReplicaDeletionIneligible
			}
		}
	}

}

func (this *ReplicaStateMachine) shutdown() {

}
