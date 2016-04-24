package main

type ControllerContext struct {
	liveBrokers                []Broker
	allTopics                  []string
	partitionReplicaAssignment map[TopicAndPartition][]int
	partitionLeadershipInfo    map[TopicAndPartition]LeaderIsrAndControllerEpoch
}
