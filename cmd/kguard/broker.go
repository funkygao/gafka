package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
)

var _ Executor = &MonitorBrokers{}

// MonitorBrokers monitors aliveness of brokers.
type MonitorBrokers struct {
	zkzone *zk.ZkZone
	stop   chan struct{}
	tick   time.Duration
	wg     *sync.WaitGroup
}

func (this *MonitorBrokers) Init() {}

func (this *MonitorBrokers) Run() {
	defer this.wg.Done()

	ticker := time.NewTicker(this.tick)
	defer ticker.Stop()

	deadBrokers := metrics.NewRegisteredGauge("brokers.dead", nil)
	unregisteredBrokers := metrics.NewRegisteredGauge("brokers.unreg", nil)
	for {
		select {
		case <-this.stop:
			return

		case <-ticker.C:
			dead, unregistered := this.report()
			deadBrokers.Update(dead)
			unregisteredBrokers.Update(unregistered)
		}
	}
}

func (this *MonitorBrokers) report() (dead, unregistered int64) {
	this.zkzone.ForSortedBrokers(func(cluster string, liveBrokers map[string]*zk.BrokerZnode) {
		zkcluster := this.zkzone.NewCluster(cluster)
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
