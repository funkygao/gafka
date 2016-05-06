package kafka

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/watchers"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
)

var _ watchers.Watcher = &WatchClusters{}

// WatchClusters montor num of kafka clusters over the time.
type WatchClusters struct {
	Zkzone *zk.ZkZone
	Stop   chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchClusters) Init() {}

func (this *WatchClusters) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	clusters := metrics.NewRegisteredGauge("clusters", nil)
	for {
		select {
		case <-this.Stop:
			return

		case <-ticker.C:
			clusters.Update(int64(len(this.Zkzone.Clusters())))
		}
	}

}
