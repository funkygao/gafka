package kateway

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/watchers"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
)

var _ watchers.Watcher = &SubLag{}

// SubLag monitors aliveness of kateway cluster.
type SubLag struct {
	Zkzone *zk.ZkZone
	Stop   chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	zkcluster *zk.ZkCluster
}

func (this *SubLag) Init() {}

func (this *SubLag) Run() {
	defer this.Wg.Done()

	this.zkcluster = this.Zkzone.NewCluster("bigtopic")

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	subLagGroups := metrics.NewRegisteredGauge("sub.lags", nil)
	for {
		select {
		case <-this.Stop:
			return

		case <-ticker.C:
			subLagGroups.Update(int64(this.report()))

		}
	}
}

func (this *SubLag) report() int {
	return 0
}
