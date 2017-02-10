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
	monitor.RegisterWatcher("kafka.zombiecg", func() monitor.Watcher {
		return &WatchZombieConsumerGroups{
			Tick: time.Minute,
		}
	})
}

type WatchZombieConsumerGroups struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchZombieConsumerGroups) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchZombieConsumerGroups) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	zombies := metrics.NewRegisteredGauge("consumer.zombie", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("kafka.zombiecg stopped")
			return

		case <-ticker.C:
			n := 0
			this.Zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
				zbcg := zkcluster.ZombieConsumerGroups(false)
				log.Warn("cluster[%s] zombies: %+v", zkcluster.Name(), zbcg)

				n += len(zbcg)
			})

			zombies.Update(int64(n))
		}
	}

}
