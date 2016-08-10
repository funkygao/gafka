// Package job implements the schedulable message(job) underlying storage.
package job

import (
	"fmt"
	"time"
)

type JobItem struct {
	AppId   int
	JobId   int64
	Payload []byte
	Ctime   time.Time
	DueTime int64
}

func (this JobItem) String() string {
	return fmt.Sprintf("%d: %s", this.JobId, string(this.Payload))
}

func (this JobItem) PayloadString(limit int) string {
	if limit > 0 && len(this.Payload) > limit {
		return string(this.Payload[:limit+1])
	}

	return string(this.Payload)
}
