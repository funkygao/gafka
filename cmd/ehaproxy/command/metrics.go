package command

import (
	"time"

	"github.com/funkygao/go-metrics"
	//log "github.com/funkygao/log4go"
)

type haproxyMetrics struct {
	ctx *Start

	interval time.Duration
	uri      string

	pubBin metrics.Gauge
}

func (this *haproxyMetrics) start() {
	if this.ctx == nil {
		panic("nil ctx not allowed")
	}

	tick := time.NewTicker(this.interval)
	defer tick.Stop()

	for {
		select {
		case <-this.ctx.quitCh:
			return

		case <-tick.C:
			this.reportStats()
		}
	}
}

func (this *haproxyMetrics) reportStats() {

}
