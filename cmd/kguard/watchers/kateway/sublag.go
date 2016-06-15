package kateway

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/watchers"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
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

	this.zkcluster = this.Zkzone.NewCluster("bigtopic") // TODO

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

func (this *SubLag) report() (lags int) {
	for group, consumers := range this.zkcluster.ConsumersByGroup("") {
		for _, c := range consumers {
			if !c.Online {
				continue
			}

			// offset commit every 1m, sublag runs every 1m, so the gap might be 2m
			if c.Lag > 0 && time.Since(c.Mtime.Time()) >= time.Minute*3 {
				log.Warn("group[%s] topic[%s/%s] %d - %d = %d", group, c.Topic, c.PartitionId,
					c.ProducerOffset, c.ConsumerOffset, c.Lag)
				lags++
			}
		}
	}

	return
}
