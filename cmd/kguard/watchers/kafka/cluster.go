package kafka

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kafka.cluster", func() monitor.Watcher {
		return &WatchClusters{
			Tick: time.Minute,
		}
	})
}

// WatchClusters montor num of kafka clusters over the time.
type WatchClusters struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchClusters) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchClusters) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	clusters := metrics.NewRegisteredGauge("clusters", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("kafka.cluster stopped")
			return

		case <-ticker.C:
			clusters.Update(int64(len(this.Zkzone.Clusters())))
		}
	}

}
