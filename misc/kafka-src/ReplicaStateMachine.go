package main

type ReplicaStateMachine struct {
	controller *KafkaController

	replicaState map[PartitionAndReplica]ReplicaState
}

func (this *ReplicaStateMachine) registerListeners() {

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
