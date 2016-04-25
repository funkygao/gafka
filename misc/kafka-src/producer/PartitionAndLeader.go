package main

type PartitionAndLeader struct {
	topic          string
	partitionId    int32
	leaderBrokerId int
}
