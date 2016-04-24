package main

type KafkaRequestHandlerPool struct {
}

type TopicAndPartition struct {
	Topic       string
	PartitionId int32
}

type Broker struct {
	id   int
	host string
	port int
}

type PartitionAndReplica struct {
	Topic       string
	PartitionId int32
	Replica     int
}

type ReplicaState byte
