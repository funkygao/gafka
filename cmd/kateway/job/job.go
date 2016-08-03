// Package job implements the schedulable message(job) underlying storage.
package job

import (
	"time"
)

// JobStore is the backend storage layer for jobs(schedulable message).
type JobStore interface {

	// Name returns the underlying storage name.
	Name() string

	Start() error
	Stop()

	// Add pubs a schedulable message(job) synchronously.
	Add(cluster, topic string, payload []byte, delay time.Duration) (jobId string, err error)

	// Delete removes a job by jobId.
	Delete(cluster, topic, jobId string) (err error)
}

var Default JobStore
