package zk

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("influx.query", func() monitor.Watcher {
		return &WatchInfluxDB{
			Tick: time.Minute,
		}
	})
}

// WatchInfluxDB continuously query InfluxDB for major metrics.
type WatchInfluxDB struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchInfluxDB) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.WaitGroup()
}

func (this *WatchInfluxDB) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	for {
		select {
		case <-this.Stop:
			log.Info("influx.query stopped")
			return

		case <-ticker.C:

		}
	}
}
