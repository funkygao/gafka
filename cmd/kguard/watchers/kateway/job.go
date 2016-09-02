package kateway

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kateway.job", func() monitor.Watcher {
		return &WatchJob{}
	})
}

// WatchJob monitors aliveness of kateway cluster.
type WatchJob struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Wg     *sync.WaitGroup
}

func (this *WatchJob) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchJob) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	jobs := metrics.NewRegisteredGauge("kateway.jobs", nil)
	webhooks := metrics.NewRegisteredGauge("kateway.webhooks", nil)
	actors := metrics.NewRegisteredGauge("kateway.actors", nil)

	for {
		select {
		case <-this.Stop:
			log.Info("kateway.job stopped")
			return

		case <-ticker.C:
			jobQueues := this.Zkzone.ChildrenWithData(zk.PubsubJobQueues)
			jobs.Update(int64(len(jobQueues)))

			webhoookRegistered := this.Zkzone.ChildrenWithData(zk.PubsubWebhooks)
			webhooks.Update(int64(len(webhoookRegistered)))

			actorsActive := this.Zkzone.ChildrenWithData(zk.PubsubActors)
			actors.Update(int64(len(actorsActive)))
		}
	}
}
