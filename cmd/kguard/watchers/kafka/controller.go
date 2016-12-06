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
	monitor.RegisterWatcher("kafka.controller", func() monitor.Watcher {
		return &WatchControllers{
			Tick: time.Minute,
		}
	})
}

type WatchControllers struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	controllers map[string]time.Time
}

func (this *WatchControllers) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchControllers) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	this.controllers = make(map[string]time.Time)
	controllerJitter := metrics.NewRegisteredGauge("kafka.controller.jitter", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("kafka.controller stopped")
			return

		case <-ticker.C:
			controllerJitter.Update(this.report())

		}
	}
}

func (this *WatchControllers) report() (jitter int64) {
	this.Zkzone.ForSortedControllers(func(cluster string, controller *zk.ControllerMeta) {
		if mtime, present := this.controllers[cluster]; !present {
			this.controllers[cluster] = controller.Mtime.Time()
		} else if mtime != controller.Mtime.Time() {
			log.Warn("controller[%s] jitter", cluster)
			jitter++

			this.controllers[cluster] = controller.Mtime.Time()
		}
	})

	return
}
