package main

// Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
type Partition struct {
	topic             string
	partitionId       int32
	replicationFactor int

	replicaManager *ReplicaManager
}

func (this *Partition) isUnderReplicated() bool {

}
