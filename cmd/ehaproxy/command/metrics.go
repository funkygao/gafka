package command

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	metrics "github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

var (
	ErrInvalidStatsRsp = errors.New("invalid stats response")
	ErrEmptyStats      = errors.New("empty stats")
	ErrUnsupService    = errors.New("unsupported service")
	ErrMetricsNotFound = errors.New("metrics not found")
)

type haproxyMetrics struct {
	ctx *Start

	interval time.Duration
	uri      string

	isInited      bool
	svcMetricsMap map[string]*servicMetrics //pub:..., sub:..., ...

}

type servicMetrics struct {
	svcName    string                   //pub, sub
	metricsMap map[string]metrics.Gauge //rate:gauge, ..
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

	//get stats
	header, records, err := this.getStats(this.uri)
	if err != nil {
		log.Error("uri: %s, get stats error, %v", this.uri, err)
		return
	}

	//init if needed
	err = this.initMetrics(header, records)
	if err != nil {
		log.Error("uri: %s, init metrics error, %v", this.uri, err)
		return
	}

	//update metrics
	err = this.updateMetrics(records)
	if err != nil {
		log.Error("uri: %s, update metrics error, %v", this.uri, err)
		return
	}
}

func (this *haproxyMetrics) getStats(statsUri string) (header []string,
	records map[string]map[string]int64, err error) {

	client := http.Client{Timeout: time.Second * 30}
	resp, err := client.Get(statsUri)
	if err != nil {
		log.Error("fetch[%s] error: %v", statsUri, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Error("fetch[%s] stats:[%d] not ok", statsUri, resp.StatusCode)
		err = ErrInvalidStatsRsp
		return
	}

	reader := json.NewDecoder(resp.Body)
	err = reader.Decode(&records)
	if err != nil {
		log.Error("fetch[%s] stats body[%s] decode err: %v", statsUri, resp.Body, err)
		return
	}

	if len(records) <= 0 {
		log.Error("fetch[%s] stats body[%s] empty content", statsUri, resp.Body)
		err = ErrEmptyStats
		return
	}

	//get header
	for _, svcCols := range records {
		for colName := range svcCols {
			header = append(header, colName)
		}
		//only use the firet record to build header
		break
	}

	return
}

func (this *haproxyMetrics) initMetrics(header []string,
	records map[string]map[string]int64) (err error) {

	if this.isInited {
		return
	}

	//init svc
	this.svcMetricsMap = make(map[string]*servicMetrics)
	for svcName := range records {
		//add new service metrics to map
		svcMetrics := newServiceMetrics(svcName, header)
		this.svcMetricsMap[svcName] = svcMetrics
	}

	this.isInited = true
	return
}

func newServiceMetrics(svcName string, header []string) *servicMetrics {

	//svcMetrics.svcName = svcName
	svcMetrics := servicMetrics{svcName: svcName}
	svcMetrics.metricsMap = make(map[string]metrics.Gauge)
	for _, colName := range header {
		fullMetricName := "haproxy." + svcName + "." + colName
		svcMetrics.metricsMap[colName] = metrics.NewRegisteredGauge(fullMetricName, nil)
	}
	return &svcMetrics
}

func (this *haproxyMetrics) updateMetrics(records map[string]map[string]int64) (err error) {

	for svcName, svcCols := range records {
		//find svc
		svcMetrics, present := this.svcMetricsMap[svcName]
		if !present {
			log.Error("svcName[%s] not in svcMetricsMap[%#v]", svcName, this.svcMetricsMap)
			err = ErrUnsupService
			return err
		}

		//update col value
		for colName, colVal := range svcCols {

			//find gague
			gauge, present := svcMetrics.metricsMap[colName]
			if !present {
				log.Error("colName[%s] not in metricsMap[%#v]", colName, svcMetrics.metricsMap)
				err = ErrMetricsNotFound
				return err
			}

			//update gauge
			gauge.Update(colVal)
		}

	}

	return
}
