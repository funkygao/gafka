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

	// warmup
	kws, _ := this.Zkzone.KatewayInfos()
	liveKateways.Update(int64(len(kws)))

	for {
		select {
		case <-this.Stop:
			return

		case <-ticker.C:
			kws, _ := this.Zkzone.KatewayInfos()
			liveKateways.Update(int64(len(kws)))
		}
	}
}
