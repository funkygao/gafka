package mysql

import (
	"fmt"
	"strconv"
	"time"

	"github.com/funkygao/fae/config"
	"github.com/funkygao/fae/servant/mysql"
	"github.com/funkygao/gafka/cmd/kateway/job"
	"github.com/funkygao/golib/idgen"
)

const (
	sqlAdd = "INSERT INTO " // FIXME
	sqlDel = ""
)

type mysqlStore struct {
	idgen *idgen.IdGenerator
	mc    *mysql.MysqlCluster
}

func New(id string, cf *config.ConfigMysql) (job.JobStore, error) {
	if cf == nil {
		return nil, fmt.Errorf("job store: empty mysql config")
	}

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
		mc:    mysql.New(cf),
	}, nil
}

func (this *mysqlStore) CreateJob(cluster, topic string) (err error) {
	return
}

func (this *mysqlStore) Add(cluster, topic string, payload []byte, delay time.Duration) (jobId string, err error) {
	jid := this.nextId()
	_, _, err = this.mc.Exec(cluster, topic, int(jid), sqlAdd, jid, payload)
	jobId = strconv.FormatInt(jid, 10)
	return
}

func (this *mysqlStore) Delete(cluster, topic, jobId string) (err error) {
	// TODO race condition with actor worker
	var jid int64
	jid, err = strconv.ParseInt(jobId, 10, 64)
	if err != nil {
		return
	}
	_, _, err = this.mc.Exec(cluster, topic, int(jid), sqlDel, jid)

	return
}

func (this *mysqlStore) Name() string {
	return "mysql"
}

func (this *mysqlStore) Start() error {
	this.mc.Warmup()
	return nil
}

func (this *mysqlStore) Stop() {
	this.mc.Close()
}
