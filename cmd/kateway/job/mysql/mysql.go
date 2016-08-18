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
	lookupPool     = "ShardLookup"
	appLookupTable = "AppLookup"
	AppPool        = "AppShard"

	sqlInsertAppLookup = "INSERT IGNORE INTO AppLookup(entityId, shardId, name, ctime) VALUES(?,?,?,?)"
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

	cf.DefaultLookupTable = appLookupTable
	return &mysqlStore{
		idgen: ig,
		mc:    mysql.New(cf),
	}, nil
}

func (this *mysqlStore) CreateJobQueue(shardId int, appid, topic string) (err error) {
	// first, insert into app if not present
	aid, table := App_id(appid), JobTable(topic)
	_, _, err = this.mc.Exec(lookupPool, appLookupTable, 0, sqlInsertAppLookup,
		aid, shardId, appid, time.Now())
	if err != nil {
		return
	}

	// create the job table and job histrory table
	// in mysql InnoDB, blob is []byte while text is string, both length limit 1<<16(64KB)
	sql := fmt.Sprintf(`
CREATE TABLE %s (
    job_id bigint unsigned NOT NULL DEFAULT 0,
    payload blob,
    ctime int NOT NULL DEFAULT 0,
    mtime int NOT NULL DEFAULT 0,
    due_time int NOT NULL,
    PRIMARY KEY (job_id),
    KEY(due_time)
) ENGINE = INNODB DEFAULT CHARSET utf8
		`, table)
	_, _, err = this.mc.Exec(AppPool, table, aid, sql)
	if err != nil {
		return
	}

	historyTable := HistoryTable(topic)
	sql = fmt.Sprintf(`
CREATE TABLE %s (
    job_id bigint unsigned NOT NULL DEFAULT 0,
    payload blob NOT NULL,
    ctime int NOT NULL DEFAULT 0,    
    due_time int NOT NULL,
    etime int NOT NULL DEFAULT 0,
    actor_id char(64) NOT NULL,
    PRIMARY KEY (job_id),
    KEY(due_time)
) ENGINE = INNODB DEFAULT CHARSET utf8
		`, historyTable)
	_, _, err = this.mc.Exec(AppPool, historyTable, aid, sql)
	return
}

func (this *mysqlStore) Add(appid, topic string, payload []byte, delay time.Duration) (jobId string, err error) {
	jid := this.nextId()
	table, aid := JobTable(topic), App_id(appid)
	now := time.Now()
	sql := fmt.Sprintf("INSERT INTO %s(job_id, payload, ctime, due_time) VALUES(?,?,?,?)", table)
	_, _, err = this.mc.Exec(AppPool, table, aid, sql,
		jid, payload, now.Unix(), now.Add(delay).Unix())
	jobId = strconv.FormatInt(jid, 10)
	return
}

func (this *mysqlStore) Delete(appid, topic, jobId string) (err error) {
	var jid int64
	jid, err = strconv.ParseInt(jobId, 10, 64)
	if err != nil {
		return
	}

	var affectedRows int64
	table, aid := JobTable(topic), App_id(appid)
	sql := fmt.Sprintf("DELETE FROM %s WHERE job_id=?", table) // TODO what if Alice delete Bob's job?
	affectedRows, _, err = this.mc.Exec(AppPool, table, aid, sql, jid)
	if err == nil && affectedRows == 0 {
		err = job.ErrNothingDeleted
	}

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
