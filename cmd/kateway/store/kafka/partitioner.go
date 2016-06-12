package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
)

var (
	excludedPartitions     = make(map[string]map[int32]struct{}, 50) // topic:partition
	excludedPartitionsLock sync.RWMutex
)

type exclusivePartitioner struct {
	hasher sarama.Partitioner
}

func NewExclusivePartitioner(topic string) sarama.Partitioner {
	this := &exclusivePartitioner{
		hasher: sarama.NewHashPartitioner(topic),
	}

	return this
}

func (this *exclusivePartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (partitionId int32, err error) {
	if numPartitions == 1 {
		return 0, nil
	}

	partitionId, err = this.hasher.Partition(message, numPartitions)

	excludedPartitionsLock.RLock()
	deadPartitions := excludedPartitions[message.Topic]
	excludedPartitionsLock.RUnlock()

	if err != nil || len(deadPartitions) == 0 || len(deadPartitions) == int(numPartitions) {
		// all partitions dead? I have to pick one!
		return
	}

	for {
		if _, present := deadPartitions[partitionId]; !present {
			// bingo!
			return
		}

		partitionId = (partitionId + 1) % numPartitions
	}

}

func (this *exclusivePartitioner) RequiresConsistency() bool {
	return true
}
