package command

import (
	"encoding/csv"
	"net/http"
	"time"

	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
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
	client := http.Client{Timeout: time.Second * 30}
	resp, err := client.Get(this.uri)
	if err != nil {
		log.Error("fetch stats: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Error("fetch stats got status: %d", resp.StatusCode)
		return
	}

	reader := csv.NewReader(resp.Body)
	records, err := reader.ReadAll()
	if err != nil {
		log.Error("fetch stats csv: %v", err)
		return
	}

	for _, r := range records {
		log.Info("%d %+v", len(r), r)
	}
}
