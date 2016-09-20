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
	monitor.RegisterWatcher("kateway.topics", func() monitor.Watcher {
		return &WatchTopics{
			Tick: time.Minute,
		}
	})
}

// WatchTopics monitors count of PubSub topics.
type WatchTopics struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchTopics) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchTopics) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	topicsMetric := metrics.NewRegisteredGauge("kateway.topics", nil)

	for {
		select {
		case <-this.Stop:
			log.Info("kateway.kateway stopped")
			return

		case <-ticker.C:
			clusters := this.Zkzone.PublicClusters()
			n := 0
			for _, zkcluster := range clusters {
				n += len(zkcluster.TopicsCtime())
			}
			topicsMetric.Update(int64(n))
		}
	}
}
