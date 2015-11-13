package zk

type Consumer struct {
	Online      bool
	Topic       string
	PartitionId string
	Offset      int64
	Lag         int64
}
