package command

import (
	"errors"
	"time"
	//log "github.com/funkygao/log4go"
)

var (
	ErrUnsupMetricsType      = errors.New("unsupported metrics type")
	ErrUnsupGaugeMetrics     = errors.New("unsupported gauge metrics")
	ErrUnsupMeterMetrics     = errors.New("unsupported meter metrics")
	ErrMetricsColumnNotFound = errors.New("metrics column not found")
)

type haproxyMetrics struct {
	ctx *Start

	interval time.Duration
	uri      string

	colMapSetted    bool
	colMap          map[string]int           //rate:33, scur:4, ...
	proxyMetricsMap map[string]*proxyMetrics //pub.frontend:, pub.backend:, ...

}

type proxyMetrics struct {
	proxyName   string                     //pub, sub
	proxyType   string                     //FRONTEND, BACKEND
	metricsDefs []metricsDefine            //session.rate, session.pctg, ...
	metricsMap  map[string]*metricsWrapper //session.rate:gauge, ..
}

type metricsDefine struct {
	metricsName string //session.rate, session.pctg,...
	metricsType string //Gauge, Meter, ...
}

type metricsWrapper struct {
	meterBase        int64
	metricsCollector interface{} //Gauge, Meter, ...
}

func (this *haproxyMetrics) start() {
	if this.ctx == nil {
		panic("nil ctx not allowed")
	}

	tick := time.NewTicker(this.interval)
	defer tick.Stop()

	//init haproxy metrics settings
	// err := this.init()
	// if err != nil {
	// 	log.Error("haproxyMetrics init, %v", err)
	// 	return
	// }
	// log.Info("haproxyMetrics init succ")

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
