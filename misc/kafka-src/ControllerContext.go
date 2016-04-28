package main

type ControllerContext struct {
	epoch          int
	epochZkVersion int

	liveBrokers                []Broker
	allTopics                  []string
	partitionReplicaAssignment map[TopicAndPartition][]int
	partitionLeadershipInfo    map[TopicAndPartition]LeaderIsrAndControllerEpoch
}
