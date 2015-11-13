package zk

type Consumer struct {
	Online         bool
	Topic          string
	PartitionId    string
	ConsumerOffset int64
	ProducerOffset int64
	Lag            int64
}
