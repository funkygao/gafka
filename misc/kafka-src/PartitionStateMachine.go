package main

type PartitionStateMachine struct {
	zkClient   *ZkClient
	controller *KafkaController
}

func (this *PartitionStateMachine) registerListeners() {

}

func (this *PartitionStateMachine) Startup() {

}

func (this *PartitionStateMachine) registerPartitionChangeListener(topic string) {
	this.zkClient.subscribeDataChanges("/brokers/topics/"+topic, this.addPartitionListener)
}

func (this *PartitionStateMachine) addPartitionListener(topic string) {
	this.zkClient.getReplicaAssignmentForTopics([]string{topic})
	var partitionsToBeAdded []TopicAndPartition
	this.controller.onNewPartitionCreation(partitionsToBeAdded)
}

func (this *PartitionStateMachine) handleStateChanges(partitions []TopicAndPartition) {
	for _, tp := range partitions {

	}
}

func (this *PartitionStateMachine) handleStateChange(topic string, partitionId int32, state PartitionState) {

}

func (this *PartitionStateMachine) shutdown() {

}
