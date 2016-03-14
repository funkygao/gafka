package sla

import (
	"fmt"
	"strconv"
)

const (
	SlaKeyRetentionHours = "retention.hours"
	SlaKeyRetentionBytes = "retention.bytes"
	SlaKeyPartitions     = "partitions"
	SlaKeyReplicas       = "replicas"

	SlaKeyRetryTopic      = "retry"
	SlaKeyDeadLetterTopic = "dead"
)

const (
	defaultRetentionBytes = -1     // unlimited
	defaultRetentionHours = 7 * 24 // 7 days
	defaultPartitions     = 1
	defaultReplicas       = 2

	maxReplicas       = 3
	maxPartitions     = 20
	maxRetentionHours = 7 * 24
)

type TopicSla struct {
	RetentionHours float64
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

func (this *TopicSla) ParseRetentionHours(s string) error {
	if len(s) == 0 {
		return ErrEmptyArg
	}

	f, e := strconv.ParseFloat(s, 64)
	if e != nil {
		return ErrNotNumber
	}

	if f < 0 {
		return ErrNegative
	}

	this.RetentionHours = f

	return nil
}

// Dump the sla for kafka-topics.sh as arguments.
func (this *TopicSla) DumpForTopicsCli() []string {
	r := make([]string, 0)
	if this.Partitions < 1 || this.Partitions > maxPartitions {
		this.Partitions = defaultPartitions
	}
	r = append(r, fmt.Sprintf("--partitions %d", this.Partitions))
	if this.Replicas != defaultReplicas && this.Replicas > 0 && this.Replicas <= maxReplicas {
		r = append(r, fmt.Sprintf("--replication-factor %d", this.Replicas))
	}
	if this.RetentionBytes != defaultRetentionBytes && this.RetentionBytes > 0 {
		r = append(r, fmt.Sprintf("--config retention.bytes=%d", this.RetentionBytes))
	}
	if this.RetentionHours != defaultRetentionHours && this.RetentionHours > 0 && this.RetentionHours <= maxRetentionHours {
		r = append(r, fmt.Sprintf("--config retention.ms=%d",
			int(this.RetentionHours*1000*3600)))
	}
	return r
}
