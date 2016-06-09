package store

import (
	"time"
)

// A PubStore is a generic store that can Pub sync/async.
type PubStore interface {
	// Name returns the name of the underlying store.
	Name() string

	Start() error
	Stop()

	// AddJob pub a schedulable message(job) synchronously.
	AddJob(cluster, topic string, payload []byte, delay time.Duration) (jobId string, err error)

	// DeleteJob removes a job by jobId.
	DeleteJob(cluster, jobId string) error

	// SyncPub pub a keyed message to a topic of a cluster synchronously.
	SyncPub(cluster, topic string, key, msg []byte) (partition int32, offset int64, err error)

	// SyncAllPub pub a keyed message to all replicas before sending response.
	SyncAllPub(cluster, topic string, key, msg []byte) (partition int32, offset int64, err error)

	// AsyncPub pub a keyed message to a topic of a cluster asynchronously.
	AsyncPub(cluster, topic string, key, msg []byte) (partition int32, offset int64, err error)

	// MarkPartitionsDead will disable a topic's partitions.
	MarkPartitionsDead(topic string, partitionIds map[int32]struct{})
}

var DefaultPubStore PubStore
