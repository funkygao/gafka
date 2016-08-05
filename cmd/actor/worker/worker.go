package worker

import (
	"strings"

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

	c chan job.JobItem
}

func New(cluster, topic string, mc *mysql.MysqlCluster, stopper <-chan struct{}) *Worker {
	return &Worker{
		cluster: cluster,
		topic:   topic,
		mc:      mc,
		stopper: stopper,
	}
}

// poll mysql for due jobs and send to kafka.
func (this *Worker) Run() {
	p := strings.SplitN(this.topic, ".", 1)
	aid := jm.App_id(p[0])

	log.Trace("starting")

	var ji job.JobItem
	sql := ""
	rows, err := this.mc.Query(jm.AppPool, this.topic, aid, sql, []interface{}{})
	if err != nil {
		log.Error(err)
		return
	}
	for rows.Next() {
		err = rows.Scan(&ji.AppId, &ji.JobId, &ji.DueTime, &ji.Payload)
		if err == nil {
			this.c <- ji
		}
	}

	rows.Close()
}

func (this *Worker) m() {
	var batch int64
	for {
		select {
		case ji := <-this.c:
			batch += 1
			if batch%int64(100) == 0 {
				// update table set xx where id in ()
			}
			store.DefaultPubStore.SyncPub(this.cluster, this.topic, nil, ji.Payload)

		}
	}
}
