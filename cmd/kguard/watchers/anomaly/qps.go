package anomaly

import (
	"sync"
	"time"

	"github.com/funkygao/anomalyzer"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("anomaly.qps", func() monitor.Watcher {
		return &WatchQps{
			Tick: time.Minute,
		}
	})
}

// WatchQps watches zookeeper health.
type WatchQps struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	anomaly anomalyzer.Anomalyzer
}

func (this *WatchQps) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()

	conf := &anomalyzer.AnomalyzerConf{
		Sensitivity: 0.1,
		UpperBound:  5,
		LowerBound:  0,
		ActiveSize:  1,
		NSeasons:    4,
		Methods:     []string{"diff", "fence", "highrank", "lowrank", "magnitude"},
	}
	var err error
	this.anomaly, err = anomalyzer.NewAnomalyzer(conf, nil)
	if err != nil {
		panic(err)
	}
}

func (this *WatchQps) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	for {
		select {
		case <-this.Stop:
			log.Info("anomaly.qps stopped")
			return

		case <-ticker.C:

		}
	}
}
