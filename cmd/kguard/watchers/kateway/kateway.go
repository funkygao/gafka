package kateway

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/watchers"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
)

var _ watchers.Watcher = &WatchKateway{}

// WatchKateway monitors aliveness of kateway cluster.
type WatchKateway struct {
	Zkzone *zk.ZkZone
	Stop   chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchKateway) Init() {}

func (this *WatchKateway) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	liveKateways := metrics.NewRegisteredGauge("kateway.live", nil)
	for {
		select {
		case <-this.Stop:
			return

		case <-ticker.C:
			liveKateways.Update(1)
		}
	}
}
