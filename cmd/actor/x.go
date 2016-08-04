package x

import (
	"strings"

	"github.com/funkygao/fae/servant/mysql"
	"github.com/funkygao/gafka/cmd/kateway/job"
	jm "github.com/funkygao/gafka/cmd/kateway/job/mysql"
	"github.com/funkygao/gafka/cmd/kateway/store"
	"github.com/funkygao/gafka/zk"
)

type foobar struct {
	cluster, topic string
	mc             *mysql.MysqlCluster

	c chan job.JobItem
}

// poll mysql for due jobs and send to kafka.
func (this *foobar) xyz() {
	p := strings.SplitN(this.topic, ".", 1)
	aid := jm.App_id(p[0])

	var ji job.JobItem
	rows, err := this.mc.Query(jm.AppPool, this.topic, aid, sql, args)
	for rows.Next() {
		err = rows.Scan(&ji.AppId, &ji.JobId, &ji.DueTime, &ji.Payload)
		if err == nil {
			this.c <- ji
		}
	}

	rows.Close()
}

func (this *foobar) m() {
	var batch int64
	for {
		select {
		case ji := <-this.c:
			batch += 1
			if batch % 100 {
				// update table set xx where id in ()
			}
			store.DefaultPubStore.SyncPub(this.cluster, this.topic, nil, ji.Payload)

		}
	}
}
