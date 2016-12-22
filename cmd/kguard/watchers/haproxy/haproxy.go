package haproxy

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("haproxy.haproxy", func() monitor.Watcher {
		return &WatchHaproxy{
			Tick: time.Minute,
		}
	})
}

// WatchHaproxy watches haproxy health.
type WatchHaproxy struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchHaproxy) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchHaproxy) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	instances := metrics.NewRegisteredGauge("haproxy.instances", nil)

	for {
		select {
		case <-this.Stop:
			log.Info("haproxy.haproxy stopped")
			return

		case <-ticker.C:
			instances.Update(2) // TODO

		}
	}
}
