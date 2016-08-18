// Package job implements the schedulable message(job) underlying storage.
package job

import (
	"fmt"
)

type JobItem struct {
	JobId   int64
	Payload []byte
	Ctime   int64
	DueTime int64
}

func (this JobItem) String() string {
	return fmt.Sprintf("{%d:%d %s}", this.JobId, this.DueTime, string(this.Payload))
}

func (this JobItem) PayloadString(limit int) string {
	if limit > 0 && len(this.Payload) > limit {
		return string(this.Payload[:limit+1])
	}

	return string(this.Payload)
}
