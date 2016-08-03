package mysql

import (
	"strconv"
	"time"

	"github.com/funkygao/gafka/cmd/kateway/job"
	"github.com/funkygao/golib/idgen"
)

type mysqlStore struct {
	idgen *idgen.IdGenerator
}

func New(id string) (job.JobStore, error) {
	wid, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}

	ig, err := idgen.NewIdGenerator(wid)
	if err != nil {
		return nil, err
	}

	return &mysqlStore{
		idgen: ig,
	}, nil
}

func (this *mysqlStore) Add(cluster, topic string, payload []byte, delay time.Duration) (jobId string, err error) {
	return
}

func (this *mysqlStore) Delete(cluster, jobId string) (err error) {
	return
}

func (this *mysqlStore) Name() string {
	return "mysql"
}

func (this *mysqlStore) Start() error {
	return nil
}

func (this *mysqlStore) Stop() {}
