package mysql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/fae/config"
	"github.com/funkygao/fae/servant/mysql"
	"github.com/funkygao/gafka/cmd/kateway/job"
	"github.com/funkygao/golib/idgen"
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

	cf.DefaultLookupTable = "AppLookup"
	return &mysqlStore{
		idgen: ig,
		mc:    mysql.New(cf),
	}, nil
}

func (this *mysqlStore) CreateJob(appid, topic string) (err error) {
	return
}

func (this *mysqlStore) Add(appid, topic string, payload []byte, delay time.Duration) (jobId string, err error) {
	jid := this.nextId()
	table := this.table(topic)
	t0 := time.Now()
	t1 := t0.Add(delay)
	aid := 1
	sql := fmt.Sprintf("INSERT INTO %s(app_id, job_id, time_start, time_end, payload, ctime) VALUES(?,?,?,?,?,?)", table)
	_, _, err = this.mc.Exec("AppShard", table, aid, sql, aid, jid, t0, t1, payload, t0)
	jobId = strconv.FormatInt(jid, 10)
	return
}

func (this *mysqlStore) Delete(appid, topic, jobId string) (err error) {
	// TODO race condition with actor worker
	var jid int64
	jid, err = strconv.ParseInt(jobId, 10, 64)
	if err != nil {
		return
	}

	aid, _ := strconv.Atoi(appid)

	table := this.table(topic)
	sql := fmt.Sprintf("DELETE FROM %s WHERE app_id=? AND job_id=?", table)
	_, _, err = this.mc.Exec(appid, table, aid, sql, appid, jid)

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

func (this *mysqlStore) table(topic string) string {
	return strings.Replace(topic, ".", "_", -1)
}
