package mysqlStore

import (
	"time"

	"github.com/funkygao/gafka/cmd/kateway/job"
)

type mysqlStore struct{}

func New() job.JobStore {
	return &mysqlStore{}
}

func (this *mysqlStore) Add(cluster, topic string, payload []byte, delay time.Duration) (jobId int64, err error) {
	return
}

func (this *mysqlStore) Delete(cluster, jobId int64) (err error) {
	return
}

func (this *mysqlStore) Name() string {
	return "mysql"
}

func (this *mysqlStore) Start() error {
	return nil
}

func (this *mysqlStore) Stop() {}
