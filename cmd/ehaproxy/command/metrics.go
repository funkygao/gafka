package command

import (
	"encoding/csv"
	"errors"
	"net/http"
	"strings"
	"time"

	"reflect"

	"strconv"

	metrics "github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
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
	err := this.init()
	if err != nil {
		log.Error("haproxyMetrics init, %v", err)
		return
	}
	log.Info("haproxyMetrics init succ")

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

	for rowNo, row := range records {
		//log.Info("%d %+v", len(r), r)
		if rowNo == 0 {
			//record col position
			if !this.colMapSetted {
				for colPos, col := range row {
					this.colMap[col] = colPos
				}
			}
			this.colMapSetted = true
		} else {
			err = this.extraReportInfo(row)
			if err != nil {
				log.Error("extract report info: %v", err)
				return
			}
		}
	}
}

func (this *haproxyMetrics) init() (err error) {
	//init col postion map
	this.colMap = make(map[string]int)

	//init metrics settings
	err = this.initProxyMetrics()
	if err != nil {
		return err
	}

	return nil
}

// init proxy metrics info
func (this *haproxyMetrics) initProxyMetrics() (err error) {

	frontendMetricsDefs := []metricsDefine{
		{"session.rate", "Gauge"}, {"session.current", "Gauge"},
		{"bytes.in", "Meter"}, {"bytes.out", "Meter"},
		{"denied.req", "Meter"}, {"denied.resp", "Meter"},
		{"errors.req", "Meter"}, {"hrsp.4xx", "Meter"},
		{"hrsp.5xx", "Meter"}, {"req.rate", "Gauge"},
		{"session.pctg", "Gauge"},
	}

	backendMetricsDefs := []metricsDefine{
		{"queue.current", "Gauge"}, {"session.rate", "Gauge"},
		{"session.current", "Gauge"}, {"bytes.in", "Meter"},
		{"bytes.out", "Meter"}, {"denied.resp", "Meter"},
		{"errors.conn", "Meter"}, {"errors.resp", "Meter"},
		{"warnings.retr", "Gauge"}, {"warnings.redis", "Gauge"},
		{"queue.time.1024", "Gauge"}, {"conn.time.1024", "Gauge"},
		{"resp.time.1024", "Gauge"}, {"session.time.1024", "Gauge"},
	}

	//init proxyMetricsMap
	this.proxyMetricsMap = make(map[string]*proxyMetrics)

	//init proxy
	var pxFullName string
	var pxName string
	var pxType string
	//set pub frontend
	pxName = "pub"
	pxType = "FRONTEND"
	pubFrtProxyMetrics := proxyMetrics{proxyName: pxName, proxyType: pxType}
	pubFrtProxyMetrics.metricsMap = make(map[string]*metricsWrapper)
	pubFrtProxyMetrics.metricsDefs = make([]metricsDefine, 0)
	pubFrtProxyMetrics.metricsDefs = frontendMetricsDefs
	err = initMetrics(pubFrtProxyMetrics.metricsMap, pxName, pxType, frontendMetricsDefs)
	if err != nil {
		return err
	}
	//add pub frontend
	pxFullName = pxName + "." + pxType
	this.proxyMetricsMap[pxFullName] = &pubFrtProxyMetrics

	//set pub backend
	pxName = "pub"
	pxType = "BACKEND"
	pubBckProxyMetrics := proxyMetrics{proxyName: pxName, proxyType: pxType}
	pubBckProxyMetrics.metricsMap = make(map[string]*metricsWrapper)
	pubBckProxyMetrics.metricsDefs = make([]metricsDefine, 0)
	pubBckProxyMetrics.metricsDefs = backendMetricsDefs
	err = initMetrics(pubBckProxyMetrics.metricsMap, pxName, pxType, backendMetricsDefs)
	if err != nil {
		return err
	}
	//add pub backend
	pxFullName = pxName + "." + pxType
	this.proxyMetricsMap[pxFullName] = &pubBckProxyMetrics

	//set sub frontend
	pxName = "sub"
	pxType = "FRONTEND"
	subFrtProxyMetrics := proxyMetrics{proxyName: pxName, proxyType: pxType}
	subFrtProxyMetrics.metricsMap = make(map[string]*metricsWrapper)
	subFrtProxyMetrics.metricsDefs = make([]metricsDefine, 0)
	subFrtProxyMetrics.metricsDefs = frontendMetricsDefs
	err = initMetrics(subFrtProxyMetrics.metricsMap, pxName, pxType, frontendMetricsDefs)
	if err != nil {
		return err
	}
	//add pub frontend
	pxFullName = pxName + "." + pxType
	this.proxyMetricsMap[pxFullName] = &subFrtProxyMetrics

	//set pub backend
	pxName = "sub"
	pxType = "BACKEND"
	subBckProxyMetrics := proxyMetrics{proxyName: pxName, proxyType: pxType}
	subBckProxyMetrics.metricsMap = make(map[string]*metricsWrapper)
	subFrtProxyMetrics.metricsDefs = make([]metricsDefine, 0)
	subBckProxyMetrics.metricsDefs = backendMetricsDefs
	err = initMetrics(subBckProxyMetrics.metricsMap, pxName, pxType, backendMetricsDefs)
	if err != nil {
		return err
	}
	//add pub backend
	pxFullName = pxName + "." + pxType
	this.proxyMetricsMap[pxFullName] = &subBckProxyMetrics

	return nil
}

func initMetrics(metricsWrapperMap map[string]*metricsWrapper,
	proxyName string, proxyType string, metricsDefs []metricsDefine) (err error) {

	for _, metricsDef := range metricsDefs {
		fullMetricsName := proxyName + "." + strings.ToLower(proxyType) + "." + metricsDef.metricsName

		switch metricsDef.metricsType {
		case "Gauge":
			metricsWrapperMap[metricsDef.metricsName] =
				&metricsWrapper{metricsCollector: metrics.NewRegisteredGauge(fullMetricsName, nil)}
		case "Meter":
			metricsWrapperMap[metricsDef.metricsName] =
				&metricsWrapper{metricsCollector: metrics.NewRegisteredMeter(fullMetricsName, nil)}
		default:
			log.Warn("mstrics:%s, metrics type: %s unsupported",
				fullMetricsName, metricsDef.metricsType)
			return ErrUnsupMetricsType
		}
	}

	return nil
}

func (this *haproxyMetrics) extraReportInfo(row []string) (err error) {

	pxFullName, err := this.getProxyInfo(row)
	if err != nil {
		return err
	}

	//ignore the proxy we are not insterested in
	isTarget, pxMetrics := this.isTargetProxy(pxFullName)
	if !isTarget {
		return nil
	}

	err = this.collectAllMetrics(row, pxMetrics)

	return nil
}

func (this *haproxyMetrics) getProxyInfo(row []string) (pxFullName string, err error) {
	pxName, err := this.getProxyName(row)
	if err != nil {
		return "", err
	}

	pxType, err := this.getProxyType(row)
	if err != nil {
		return "", err
	}

	return pxName + "." + pxType, nil
}

func (this *haproxyMetrics) isTargetProxy(pxFullName string) (present bool, pxMetrics *proxyMetrics) {
	pxMetrics, present = this.proxyMetricsMap[pxFullName]
	return present, pxMetrics
}

func (this *haproxyMetrics) collectAllMetrics(row []string, pxMetrics *proxyMetrics) (err error) {
	for metricsName, metricsWp := range pxMetrics.metricsMap {
		err = this.collectOneMetrics(row, metricsName, metricsWp)
		if err != nil {
			return err
		}
	}

	return nil
}

func (this *haproxyMetrics) collectOneMetrics(row []string,
	metricsName string, metricsWp *metricsWrapper) (err error) {

	switch m := metricsWp.metricsCollector.(type) {
	case metrics.Gauge:
		err = this.collectOneGauge(row, metricsName, &m)
		if err != nil {
			return nil
		}
	case metrics.Meter:
		err = this.collectOneMeter(row, metricsName, &m, &(metricsWp.meterBase))
		if err != nil {
			return nil
		}
	default:
		otherTypeName := reflect.TypeOf(m)
		log.Error("metrics:%s, unsupported metrics collector type:%T", metricsName, otherTypeName)
		return ErrUnsupMetricsType
	}

	return nil
}

func (this *haproxyMetrics) collectOneGauge(row []string,
	metricsName string, gauge *metrics.Gauge) (err error) {

	var val int64
	switch metricsName {
	case "session.rate": //frontend, backend
		val, err = this.getSessionRate(row)
	case "session.current": //frontend, backend
		val, err = this.getSessionCurrent(row)
	case "req.rate": //frontend
		val, err = this.getReqRate(row)
	case "session.pctg": //frontend
		val, err = this.getSessionPercentage(row)
	case "queue.current": //backend
		val, err = this.getQueueCurrent(row)
	case "warnings.retr": //backend
		val, err = this.getWarningsRetr(row)
	case "warnings.redis": //backend
		val, err = this.getWarningRedis(row)
	case "queue.time.1024": //backend
		val, err = this.getQueueTime(row)
	case "conn.time.1024": //backend
		val, err = this.getConnTime(row)
	case "resp.time.1024": //backend
		val, err = this.getRespTime(row)
	case "session.time.1024": //backend
		val, err = this.getSessionTime(row)
	default:
		log.Error("metrics:%s, unsupported gauge collector", metricsName)
		return ErrUnsupGaugeMetrics
	}

	if err != nil {
		log.Error("metrics:%s, header:%#v, row:%v, %v", metricsName, this.colMap, row, err)
		return err
	}

	//update gauge
	(*gauge).Update(val)

	return nil
}

func (this *haproxyMetrics) collectOneMeter(row []string,
	metricsName string, meter *metrics.Meter, meterBase *int64) (err error) {

	var val int64
	switch metricsName {
	case "bytes.in": //frontend, backend
		val, err = this.getBytesIn(row)
	case "bytes.out": //frontend, backend
		val, err = this.getBytesOut(row)
	case "denied.req": //frontend
		val, err = this.getDeniedReq(row)
	case "denied.resp": //frontend, backend
		val, err = this.getDeniedResp(row)
	case "errors.req": //frontend
		val, err = this.getErrorsReq(row)
	case "errors.conn": //backend
		val, err = this.getErrorsConn(row)
	case "errors.resp": //backend
		val, err = this.getErrorsResp(row)
	case "hrsp.4xx": //frontend
		val, err = this.getHrsp4xx(row)
	case "hrsp.5xx": //frontend
		val, err = this.getHrsp5xx(row)
	default:
		log.Error("metrics:%s, unsupported meter collector", metricsName)
		return ErrUnsupMeterMetrics
	}

	if err != nil {
		log.Error("metrics:%s, header:%#v, row:%v, %v", metricsName, this.colMap, row, err)
		return err
	}

	//update meter
	delta := val - (*meterBase)
	if delta < 0 {
		//backward
		log.Warn("metrics:%s, delta:%d, header:%#v, row:%v, %v, meter value backward",
			metricsName, delta, this.colMap, row, err)
		delta = 0 //set delta to 0

	}

	(*meter).Mark(delta)
	*meterBase = val

	return nil
}

func (this *haproxyMetrics) getProxyName(row []string) (pxName string, err error) {

	//# pxname --> proxy.name
	colPos, present := this.colMap["# pxname"]
	if !present {
		return "", ErrMetricsColumnNotFound
	}

	pxName = row[colPos]
	return pxName, nil
}

func (this *haproxyMetrics) getProxyType(row []string) (pxType string, err error) {

	//svname --> proxy.type
	colPos, present := this.colMap["svname"]
	if !present {
		return "", ErrMetricsColumnNotFound
	}

	pxType = row[colPos]
	return pxType, nil
}

func (this *haproxyMetrics) getHAMetrics(row []string, metricsName string) (val int64, err error) {
	colPos, present := this.colMap[metricsName]
	if !present {
		return 0, ErrMetricsColumnNotFound
	}

	valStr := row[colPos]
	val, err = strconv.ParseInt(valStr, 10, 64)

	return val, err
}

func (this *haproxyMetrics) getQueueCurrent(row []string) (queueCurrent int64, err error) {

	//qcur --> queue.current
	return this.getHAMetrics(row, "qcur")
}

func (this *haproxyMetrics) getSessionRate(row []string) (sessionRate int64, err error) {

	//rate --> session.rate
	return this.getHAMetrics(row, "rate")
}

func (this *haproxyMetrics) getSessionCurrent(row []string) (sessionCurrent int64, err error) {

	//scur --> session current
	return this.getHAMetrics(row, "scur")
}

func (this *haproxyMetrics) getSessionPercentage(row []string) (sessionPctg int64, err error) {

	//scur, slim --> spct --> session.percentage
	sessionCurrent, err := this.getHAMetrics(row, "scur")
	if err != nil {
		return 0, err
	}

	sessionLimit, err := this.getHAMetrics(row, "slim")
	if err != nil {
		return 0, err
	}

	//no limit
	if sessionLimit <= 0 {
		return 0, nil
	}

	sessionPctg = (sessionCurrent * 100) / sessionLimit

	return sessionPctg, nil
}

func (this *haproxyMetrics) getBytesIn(row []string) (bytesIn int64, err error) {

	//bin --> bytes.in
	return this.getHAMetrics(row, "bin")
}

func (this *haproxyMetrics) getBytesOut(row []string) (bytesOut int64, err error) {

	//bout --> bytes.out
	return this.getHAMetrics(row, "bout")
}

func (this *haproxyMetrics) getDeniedReq(row []string) (deniedReq int64, err error) {

	//dreq --> denied.req
	return this.getHAMetrics(row, "dreq")
}

func (this *haproxyMetrics) getDeniedResp(row []string) (deniedResp int64, err error) {

	//dresp --> denied.resp
	return this.getHAMetrics(row, "dresp")
}

func (this *haproxyMetrics) getErrorsReq(row []string) (errorsReq int64, err error) {

	//ereq --> errors.req
	return this.getHAMetrics(row, "ereq")
}

func (this *haproxyMetrics) getErrorsConn(row []string) (errorsConn int64, err error) {

	//econ --> errors.conn
	return this.getHAMetrics(row, "econ")
}

func (this *haproxyMetrics) getErrorsResp(row []string) (errorsResp int64, err error) {

	//eresp --> errors.resp
	return this.getHAMetrics(row, "eresp")
}

func (this *haproxyMetrics) getWarningsRetr(row []string) (warningsRetr int64, err error) {

	//wretr --> warnings.retr
	return this.getHAMetrics(row, "wretr")
}

func (this *haproxyMetrics) getWarningRedis(row []string) (warningsRedis int64, err error) {

	//wredis --> warnings.redis
	return this.getHAMetrics(row, "wredis")
}

func (this *haproxyMetrics) getQueueTime(row []string) (queueTime int64, err error) {

	//qtime --> queue.time
	return this.getHAMetrics(row, "qtime")
}

func (this *haproxyMetrics) getConnTime(row []string) (connTime int64, err error) {

	//ctime --> conn.time
	return this.getHAMetrics(row, "ctime")
}

func (this *haproxyMetrics) getRespTime(row []string) (respTime int64, err error) {

	//rtime --> resp.time
	return this.getHAMetrics(row, "rtime")
}

func (this *haproxyMetrics) getSessionTime(row []string) (sessionTime int64, err error) {

	//ttime --> session.time
	return this.getHAMetrics(row, "ttime")
}

func (this *haproxyMetrics) getHrsp4xx(row []string) (hrsp4xx int64, err error) {

	//hrsp_4xx --> hrsp.4xx
	return this.getHAMetrics(row, "hrsp_4xx")
}

func (this *haproxyMetrics) getHrsp5xx(row []string) (hrsp5xx int64, err error) {

	//hrsp_5xx --> hrsp.5xx
	return this.getHAMetrics(row, "hrsp_5xx")
}

func (this *haproxyMetrics) getReqRate(row []string) (reqRate int64, err error) {

	//req_rate --> req.rate
	return this.getHAMetrics(row, "req_rate")
}
