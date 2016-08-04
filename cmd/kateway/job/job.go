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

	// CreateJob creates a bucket where jobs will persist.
	CreateJob(shardId int, appid, topic string) (err error)

	// Add pubs a schedulable message(job) synchronously.
	Add(appid, topic string, payload []byte, delay time.Duration) (jobId string, err error)

	// Delete removes a job by jobId.
	Delete(appid, topic, jobId string) (err error)
}

var Default JobStore
