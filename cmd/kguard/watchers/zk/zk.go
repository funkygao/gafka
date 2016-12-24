package zk

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
	monitor.RegisterWatcher("zk.zk", func() monitor.Watcher {
		return &WatchZk{
			Tick: time.Minute,
		}
	})
}

// WatchZk watches zookeeper health.
type WatchZk struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	lastReceived int64
}

func (this *WatchZk) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

// TODO monitor zk watchers count
func (this *WatchZk) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	qps := metrics.NewRegisteredGauge("zk.qps", nil)
	conns := metrics.NewRegisteredGauge("zk.conns", nil)
	znodes := metrics.NewRegisteredGauge("zk.znodes", nil)
	deadNodes := metrics.NewRegisteredGauge("zk.dead", nil)
	reelect := metrics.NewRegisteredGauge("zk.reelect", nil)
	lastLeader := ""
	for {
		select {
		case <-this.Stop:
			log.Info("zk.zk stopped")
			return

		case <-ticker.C:
			r, c, z, d, l := this.collectMetrics()
			if this.lastReceived > 0 {
				qps.Update((r - this.lastReceived) / int64(this.Tick.Seconds()))
			}
			this.lastReceived = r

			conns.Update(c)
			znodes.Update(z)
			deadNodes.Update(d)
			if lastLeader != "" && lastLeader != l {
				reelect.Update(1)
			} else {
				reelect.Update(0)
			}
			lastLeader = l
		}
	}
}

func (this *WatchZk) collectMetrics() (received, conns, znodes, dead int64, leader string) {
	for host, statOutput := range this.Zkzone.RunZkFourLetterCommand("stat") {
		stat := zk.ParseStatResult(statOutput)
		if stat.Mode == "L" {
			leader = host
		}
		n, _ := strconv.Atoi(stat.Received)
		received += int64(n)
		n, _ = strconv.Atoi(stat.Connections)
		conns += int64(n)                // sum up the total connections
		n, _ = strconv.Atoi(stat.Znodes) // each node in zk should the same amount of znode
		znodes = int64(n)
		if stat.Mode == "" {
			dead++
		}
	}

	return
}
