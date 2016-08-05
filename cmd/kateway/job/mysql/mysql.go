package mysql

import (
	"fmt"
	"hash/adler32"
	"strconv"
	"strings"
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

func (this *mysqlStore) CreateJob(shardId int, appid, topic string) (err error) {
	// first, insert into app if not present
	aid, table := App_id(appid), this.table(topic)
	_, _, err = this.mc.Exec(lookupPool, appLookupTable, 0, sqlInsertAppLookup,
		aid, shardId, appid, time.Now())
	if err != nil {
		return
	}

	// create the job table and job histrory table
	// in mysql InnoDB, blob is []byte while text is string, both length limit 1<<16(64KB)
	sql := fmt.Sprintf(`
CREATE TABLE %s (
    app_id bigint unsigned NOT NULL DEFAULT 0,
    job_id bigint unsigned NOT NULL DEFAULT 0,
    payload blob,
    ctime timestamp NOT NULL DEFAULT 0,
    mtime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    due_time timestamp NULL DEFAULT NULL COMMENT "end time point of the event",
    PRIMARY KEY (app_id, job_id),
    KEY(due_time)
) ENGINE = INNODB DEFAULT CHARSET utf8
		`, table)
	_, _, err = this.mc.Exec(AppPool, table, aid, sql)
	if err != nil {
		return
	}

	historyTable := table + "_archive"
	sql = fmt.Sprintf(`
CREATE TABLE %s (
    app_id bigint unsigned NOT NULL DEFAULT 0,
    job_id bigint unsigned NOT NULL DEFAULT 0,
    payload blob,
    ctime timestamp NOT NULL DEFAULT 0,
    due_time timestamp NULL DEFAULT NULL,
    actor_id char(64) NOT NULL,
    PRIMARY KEY (app_id, job_id),
    KEY(due_time),
    KEY(ctime)
) ENGINE = INNODB DEFAULT CHARSET utf8
		`, historyTable)
	_, _, err = this.mc.Exec(AppPool, historyTable, aid, sql)
	return
}

func (this *mysqlStore) Add(appid, topic string, payload []byte, delay time.Duration) (jobId string, err error) {
	jid := this.nextId()
	table, aid := this.table(topic), App_id(appid)
	t0 := time.Now()
	t1 := t0.Add(delay)
	sql := fmt.Sprintf("INSERT INTO %s(app_id, job_id, payload, ctime, due_time) VALUES(?,?,?,?,?)", table)
	_, _, err = this.mc.Exec(AppPool, table, aid, sql,
		aid, jid, payload, t0, t1)
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

	var affectedRows int64
	table, aid := this.table(topic), App_id(appid)
	sql := fmt.Sprintf("DELETE FROM %s WHERE app_id=? AND job_id=?", table)
	affectedRows, _, err = this.mc.Exec(AppPool, table, aid, sql,
		aid, jid)
	if affectedRows == 0 {
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

func (this *mysqlStore) table(topic string) string {
	return strings.Replace(topic, ".", "_", -1)
}

// App_id convert a string appid to hash int which is used to locate shard.
func App_id(appid string) int {
	return int(adler32.Checksum([]byte(appid)))
}
