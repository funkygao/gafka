package kafka

import (
	"strconv"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("kafka.broker", func() monitor.Watcher {
		return &WatchBrokers{
			Tick: time.Minute,
		}
	})
}

// WatchBrokers monitors aliveness of kafka brokers.
type WatchBrokers struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup
}

func (this *WatchBrokers) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchBrokers) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	deadBrokers := metrics.NewRegisteredGauge("brokers.dead", nil)
	unregisteredBrokers := metrics.NewRegisteredGauge("brokers.unreg", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("kafka.broker stopped")
			return

		case <-ticker.C:
			dead, unregistered := this.report()
			deadBrokers.Update(dead)
			unregisteredBrokers.Update(unregistered)
		}
	}
}

func (this *WatchBrokers) report() (dead, unregistered int64) {
	this.Zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		zkcluster := this.Zkzone.NewCluster(cluster)
		registeredBrokers := zkcluster.RegisteredInfo().Roster

		// find diff between registeredBrokers and liveBrokers
		// loop1 find liveBrokers>registeredBrokers
		for _, broker := range liveBrokers {
			foundInRoster := false
			for _, b := range registeredBrokers {
				bid := strconv.Itoa(b.Id)
				if bid == broker.Id && broker.Addr() == b.Addr() {
					foundInRoster = true
					break
				}
			}

			if !foundInRoster {
				unregistered += 1
			}
		}

		// loop2 find liveBrokers<registeredBrokers
		for _, b := range registeredBrokers {
			foundInLive := false
			for _, broker := range liveBrokers {
				bid := strconv.Itoa(b.Id)
				if bid == broker.Id && broker.Addr() == b.Addr() {
					foundInLive = true
					break
				}
			}

			if !foundInLive {
				dead += 1
			}
		}
	})

	return
}
