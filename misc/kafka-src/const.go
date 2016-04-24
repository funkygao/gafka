package main

const (
	RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
	HighWatermarkFilename       = "replication-offset-checkpoint"

	// Kafka protocol
	ProduceKey            int16 = 0
	FetchKey              int16 = 1
	OffsetsKey            int16 = 2
	MetadataKey           int16 = 3
	LeaderAndIsrKey       int16 = 4
	StopReplicaKey        int16 = 5
	UpdateMetadataKey     int16 = 6
	ControlledShutdownKey int16 = 7
	OffsetCommitKey       int16 = 8
	OffsetFetchKey        int16 = 9
)
