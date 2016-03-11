package sla

import (
	"fmt"
)

const (
	SlaKeyRetentionHours = "retention.hours"
	SlaKeyRetentionBytes = "retention.bytes"
	SlaKeyPartitions     = "partitions"
	SlaKeyReplicas       = "replicas"
)

const (
	defaultRetentionBytes = -1
	defaultRetentionHours = 3.
	defaultPartitions     = 1
	defaultReplicas       = 2
)

type TopicSla struct {
	RetentionHours float32
	RetentionBytes int
	Partitions     int
	Replicas       int
}

func DefaultSla() *TopicSla {
	return &TopicSla{
		RetentionBytes: -1,
		RetentionHours: defaultRetentionHours,
		Partitions:     defaultPartitions,
		Replicas:       defaultReplicas,
	}
}

func (this *TopicSla) IsDefault() bool {
	return this.Replicas == defaultReplicas &&
		this.Partitions == defaultPartitions &&
		this.RetentionBytes == defaultRetentionBytes &&
		this.RetentionHours == defaultRetentionHours
}

// Dump the sla for kafka-topics.sh as arguments.
func (this *TopicSla) DumpForTopicsCli() []string {
	r := make([]string, 0)
	if this.Partitions != defaultPartitions {
		r = append(r, fmt.Sprintf("--partitions %d", this.Partitions))
	}
	if this.Replicas != defaultReplicas {
		r = append(r, fmt.Sprintf("--replication-factor %d", this.Replicas))
	}
	if this.RetentionBytes != defaultRetentionBytes {
		r = append(r, fmt.Sprintf("--config retention.bytes=%d", this.RetentionBytes))
	}
	if this.RetentionHours != defaultRetentionHours {
		r = append(r, fmt.Sprintf("--config retention.ms=%d",
			int(this.RetentionHours*1000*3600)))
	}
	return r
}
