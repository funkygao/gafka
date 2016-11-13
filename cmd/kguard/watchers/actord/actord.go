package actord

import (
	"fmt"
	"sync"
	"time"

	"github.com/funkygao/fae/config"
	"github.com/funkygao/fae/servant/mysql"
	jm "github.com/funkygao/gafka/cmd/kateway/job/mysql"
	"github.com/funkygao/gafka/cmd/kateway/manager"
	"github.com/funkygao/gafka/cmd/kateway/manager/dummy"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("actord.actord", func() monitor.Watcher {
		return &WatchActord{
			Tick: time.Minute,
		}
	})
}

// WatchActord watches actord health and metrics.
type WatchActord struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	mc *mysql.MysqlCluster
}

func (this *WatchActord) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()

	b, err := this.Zkzone.KatewayJobClusterConfig()
	if err != nil {
		log.Error(err)
		return
	}
	var mcc = &config.ConfigMysql{}
	if err = mcc.From(b); err != nil {
		log.Error(err)
		return
	}

	this.mc = mysql.New(mcc)
	manager.Default = dummy.New("")
}

func (this *WatchActord) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	jobQueues := metrics.NewRegisteredGauge("actord.jobqueues", nil)
	actors := metrics.NewRegisteredGauge("actord.actors", nil)
	orphan := metrics.NewRegisteredGauge("actord.orphan", nil)
	backlog := metrics.NewRegisteredGauge("actord.backlog.30s", nil)
	archive := metrics.NewRegisteredGauge("actord.archive.30s", nil)

	for {
		select {
		case <-this.Stop:
			log.Info("%s stopped", this.ident())
			return

		case now := <-ticker.C:
			jLen, aLen, orphanN, backlogN, archiveN := this.watchJobs(now)
			jobQueues.Update(jLen)
			actors.Update(aLen)
			orphan.Update(orphanN)
			backlog.Update(backlogN)
			archive.Update(archiveN)

			this.watchWebhooks(now)
		}
	}
}

func (this *WatchActord) watchWebhooks(now time.Time) {
	webhoookRegistered := this.Zkzone.ChildrenWithData(zk.PubsubWebhooks)
	webhooks.Update(int64(len(webhoookRegistered)))
}

func (this *WatchActord) watchJobs(now time.Time) (jLen, aLen, orphan, backlog, archive int64) {
	jobQueues := this.Zkzone.ChildrenWithData(zk.PubsubJobQueues)
	actors := this.Zkzone.ChildrenWithData(zk.PubsubActors)
	jobQueueOwners := this.Zkzone.ChildrenWithData(zk.PubsubJobQueueOwners)

	// TODO parallel
	for topic := range jobQueues {
		b, a := this.dueJobsWithin(topic, 30, now)
		backlog += b
		archive += a
	}

	jLen = int64(len(jobQueues))
	aLen = int64(len(actors))
	orphan = jLen - int64(len(jobQueueOwners))

	return
}

func (this *WatchActord) dueJobsWithin(topic string, timeSpan int64,
	now time.Time) (backlog int64, archive int64) {
	jobTable := jm.JobTable(topic)
	appid := manager.Default.TopicAppid(topic)
	aid := jm.App_id(appid)
	sql := fmt.Sprintf("SELECT count(job_id) FROM %s WHERE due_time<=?", jobTable)
	rows, err := this.mc.Query(jm.AppPool, jobTable, aid, sql, now.Unix()+timeSpan)
	if err != nil {
		log.Error("%s: %s", this.ident(), err)
		return
	}
	var n int
	for rows.Next() {
		rows.Scan(&n)
	}
	rows.Close()
	backlog += int64(n)

	archiveTable := jm.HistoryTable(topic)
	sql = fmt.Sprintf("SELECT count(job_id) FROM %s WHERE due_time>=?", archiveTable)
	rows, err = this.mc.Query(jm.AppPool, archiveTable, aid, sql, now.Unix()-timeSpan)
	if err != nil {
		log.Error("%s: %s", this.ident(), err)
		return
	}
	for rows.Next() {
		rows.Scan(&n)
	}
	rows.Close()
	archive += int64(n)

	return

}

func (this *WatchActord) ident() string {
	return "actord.actord"
}
