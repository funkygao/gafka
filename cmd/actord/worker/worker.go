package worker

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/fae/servant/mysql"
	"github.com/funkygao/gafka/cmd/kateway/job"
	jm "github.com/funkygao/gafka/cmd/kateway/job/mysql"
	"github.com/funkygao/gafka/cmd/kateway/store"
	log "github.com/funkygao/log4go"
)

// Worker polls a single JobQueue and handle each Job.
type Worker struct {
	cluster, topic string
	mc             *mysql.MysqlCluster
	stopper        <-chan struct{}
	dueJobs        chan job.JobItem

	// cached values
	appid string
	aid   int
	table string
	ident string
}

func New(cluster, topic string, mc *mysql.MysqlCluster, stopper <-chan struct{}) *Worker {
	this := &Worker{
		cluster: cluster,
		topic:   topic,
		mc:      mc,
		stopper: stopper,
		dueJobs: make(chan job.JobItem, 200),
	}

	this.appid = topic[:strings.IndexByte(topic, '.')]
	this.aid = jm.App_id(this.appid)
	this.table = jm.JobTable(topic)
	this.ident = fmt.Sprintf("worker{cluster:%s app:%s aid:%d topic:%s table:%s}",
		this.cluster, this.appid, this.aid, this.topic, this.table)

	return this
}

// poll mysql for due jobs and send to kafka.
func (this *Worker) Run() {
	log.Trace("starting %s", this.Ident())

	var (
		wg   sync.WaitGroup
		item job.JobItem
		tick = time.NewTicker(time.Second)
		sql  = fmt.Sprintf("SELECT job_id,app_id,payload,due_time FROM %s WHERE due_time<=?", this.table)
	)

	wg.Add(1)
	go this.handleDueJobs(&wg)

	for {
		select {
		case <-this.stopper:
			log.Debug("%s stopping", this.ident)
			wg.Wait()
			return

		case now := <-tick.C:
			rows, err := this.mc.Query(jm.AppPool, this.topic, this.aid, sql, now.Unix())
			if err != nil {
				log.Error("%s: %v", this.ident, err)
				continue
			}

			for rows.Next() {
				err = rows.Scan(&item.JobId, &item.AppId, &item.Payload, &item.DueTime)
				if err == nil {
					this.dueJobs <- item
					log.Debug("%s due %s", this.ident, item)
				} else {
					log.Error("%s: %s", this.ident, err)
				}
			}

			if err = rows.Err(); err != nil {
				log.Error("%s: %s", this.ident, err)
			}

			rows.Close()
		}
	}

}

func (this *Worker) handleDueJobs(wg *sync.WaitGroup) {
	defer wg.Done()

	var (
		batch        int64
		err          error
		sqlDeleteJob = fmt.Sprintf("DELETE FROM %s WHERE job_id=?", this.table)
	)
	for {
		select {
		case <-this.stopper:
			// TODO care about the ongoing mysql insert/delete
			return

		case item := <-this.dueJobs:
			batch++
			if batch%int64(100) == 0 {
				// update table set xx where id in ()
			}

			_, _, err = store.DefaultPubStore.SyncPub(this.cluster, this.topic, nil, item.Payload)
			if err != nil {
				log.Error("%s: %s", this.ident, err)
			} else {
				// mv job to archive table
				this.mc.Exec(jm.AppPool, this.table, this.aid, sqlDeleteJob, item.JobId)
			}

		}
	}
}

func (this *Worker) Ident() string {
	return this.ident
}
