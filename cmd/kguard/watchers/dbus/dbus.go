package dbus

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("dbus.dbus", func() monitor.Watcher {
		return &WatchDbus{}
	})
}

// WatchDbus watches databus aliveness.
type WatchDbus struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Wg     *sync.WaitGroup
}

func (this *WatchDbus) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchDbus) Run() {
	defer this.Wg.Done()

	liveDbus := metrics.NewRegisteredGauge("dbus.alive", nil)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-this.Stop:
			log.Info("dbus.dbus stopped")
			return

		case <-ticker.C:
			liveDbus.Update(1) // TODO

		}
	}
}
