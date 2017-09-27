package gc

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kafka.gc", func() monitor.Watcher {
		return &WatchKafkaGC{}
	})
}

type WatchKafkaGC struct {
	Stop <-chan struct{}
	Wg   *sync.WaitGroup
}

func (this *WatchKafkaGC) Init(ctx monitor.Context) {
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchKafkaGC) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-this.Stop:
			log.Info("kafka.gc stopped")
			return

		case <-ticker.C:

		}
	}
}
