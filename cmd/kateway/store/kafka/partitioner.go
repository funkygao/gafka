package kafka

import (
	"github.com/Shopify/sarama"
)

type exclusivePartitioner struct {
	hasher           sarama.Partitioner
	deadPartitionIds map[int32]struct{}
}

func NewExclusivePartitioner(topic string) sarama.Partitioner {
	this := &exclusivePartitioner{
		hasher:           sarama.NewHashPartitioner(topic),
		deadPartitionIds: make(map[int32]struct{}),
	}
	return this
}

func (this *exclusivePartitioner) markDead(partitionIds map[int32]struct{}) {
	this.deadPartitionIds = partitionIds
}

func (this *exclusivePartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (partitionId int32, err error) {
	if numPartitions == 1 {
		return 0, nil
	}

	partitionId, err = this.hasher.Partition(message, numPartitions)
	if err != nil || len(this.deadPartitionIds) == int(numPartitions) {
		// all partitions dead? I have to pick one!
		return
	}

	for {
		if _, present := this.deadPartitionIds[partitionId]; !present {
			// bingo!
			return
		}

		partitionId = (partitionId + 1) % numPartitions
	}

}

func (this *exclusivePartitioner) RequiresConsistency() bool {
	return true
}
