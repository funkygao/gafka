package external

import (
	"bufio"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-metrics"
	"github.com/funkygao/golib/pipestream"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("zone.load", func() monitor.Watcher {
		return &WatchLoadAvg{}
	})
}

// WatchLoadAvg watches all servers load avg within a zone.
// These includes kateway/kafka/zk/, etc.
type WatchLoadAvg struct {
	Stop <-chan struct{}
	Wg   *sync.WaitGroup
}

func (this *WatchLoadAvg) Init(ctx monitor.Context) {
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchLoadAvg) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	loadHigh := metrics.NewRegisteredGauge("zone.highload", nil)

	for {
		select {
		case <-this.Stop:
			log.Info("zone.load stopped")
			return

		case <-ticker.C:
			n, err := this.highLoadCount()
			if err != nil {
				log.Error("%v", err)
			} else {
				loadHigh.Update(n)
			}

		}
	}
}

func (this *WatchLoadAvg) highLoadCount() (n int64, err error) {
	const threshold = 6.

	cmd := pipestream.New("consul", "exec",
		"uptime", "|", "grep", "load")
	err = cmd.Open()
	if err != nil {
		return
	}
	defer cmd.Close()

	scanner := bufio.NewScanner(cmd.Reader())
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		load1m, e := ctx.ExtractLoadAvg1m(line)
		if e != nil {
			continue
		}

		if load1m > threshold {
			n++
		}
	}

	return
}
