package worker

import (
	"fmt"
	"strings"
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

	dueJobs chan job.JobItem
}

func New(cluster, topic string, mc *mysql.MysqlCluster, stopper <-chan struct{}) *Worker {
	return &Worker{
		cluster: cluster,
		topic:   topic,
		mc:      mc,
		stopper: stopper,
		dueJobs: make(chan job.JobItem, 1000),
	}
}

// poll mysql for due jobs and send to kafka.
func (this *Worker) Run() {
	appid := this.topic[:strings.IndexByte(this.topic, '.')]
	aid := jm.App_id(appid)
	table := jm.JobTable(this.topic)

	log.Trace("starting worker[%d] on cluster[%s] %s/%s", aid, this.cluster, this.topic, table)

	go this.handleDueJobs()

	var ji job.JobItem
	tick := time.NewTicker(time.Second)
	sql := fmt.Sprintf("SELECT * FROM %s WHERE due_time<=?", table)
	for {

		select {
		case <-this.stopper:
			return

		case now := <-tick.C:
			rows, err := this.mc.Query(jm.AppPool, this.topic, aid, sql, []interface{}{now.Unix()})
			if err != nil {
				log.Error(err)
				return
			}

			for rows.Next() {
				err = rows.Scan(&ji.AppId, &ji.JobId, &ji.DueTime, &ji.Payload)
				if err == nil {
					this.dueJobs <- ji
				}
			}

			rows.Close()

		}
	}

}

func (this *Worker) handleDueJobs() {
	var batch int64
	for {
		select {
		case ji := <-this.dueJobs:
			batch += 1
			if batch%int64(100) == 0 {
				// update table set xx where id in ()
			}
			store.DefaultPubStore.SyncPub(this.cluster, this.topic, nil, ji.Payload)

		}
	}
}
