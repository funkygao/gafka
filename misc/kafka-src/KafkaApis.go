package main

// Logic to handle the various Kafka requests
type KafkaApis struct {
	brokerId      int
	metadataCache *MetadataCache

	zkClient       *ZkClient
	replicaManager *ReplicaManager
	controller     *KafkaController
}

func (this *KafkaApis) handle(request Request) {
	switch request.requestId {
	case ProduceKey:
		this.handleProducerRequest(request)

	case FetchKey:
		this.handleProducerRequest(request)

	case OffsetsKey:
		this.handleOffsetRequest(request)

	case MetadataKey:
		this.handleTopicMetadataRequest(request)

	case LeaderAndIsrKey:
		this.handleLeaderAndIsrRequest(request)

	case StopReplicaKey:
		this.handleStopReplicaRequest(request)

	case UpdateMetadataKey:
		this.handleUpdateMetadataRequest(request)

	case ControlledShutdownKey:
		this.handleControlledShutdownRequest(request)

	case OffsetCommitKey:
		this.handleOffsetCommitRequest(request)

	case OffsetFetchKey:
		this.handleOffsetFetchRequest(request)

	default:
		panic("")
	}

}

func (this *KafkaApis) handleProducerRequest(request Request) {
	numPartitionsInError := this.appendToLocalLog(request)
}

func (this *KafkaApis) handleFetchRequest(request Request) {
	if request.isFromFollower {
		// replication
		this.maybeUpdatePartitionHw(request)
	}
}

func (this *KafkaApis) handleOffsetRequest(request Request) {

}

func (this *KafkaApis) handleTopicMetadataRequest(request Request) {

}

func (this *KafkaApis) handleLeaderAndIsrRequest(request Request) {
	this.replicaManager.becomeLeaderOrFollower(request)
}

func (this *KafkaApis) handleStopReplicaRequest(request Request) {
	this.replicaManager.stopReplicas(request)
}

func (this *KafkaApis) handleUpdateMetadataRequest(request Request) {

}

func (this *KafkaApis) handleControlledShutdownRequest(request Request) {
	this.controller.shutdownBroker(request.brokerId)
}

//
func (this *KafkaApis) handleOffsetCommitRequest(request Request) {

}

func (this *KafkaApis) handleOffsetFetchRequest(request Request) {

}

func (this *KafkaApis) ensureTopicExists(topic string) {

}

type MetadataCache struct {
}
