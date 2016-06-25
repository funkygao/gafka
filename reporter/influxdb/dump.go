package influxdb

import (
	"fmt"
	"strings"
	"time"

	log "github.com/funkygao/log4go"
	"github.com/influxdata/influxdb/client"
	"github.com/rcrowley/go-metrics"
)

func (this *reporter) dump() (pts []client.Point) {
	var (
		tags map[string]string
		now  = time.Now()
	)
	this.reg.Each(func(name string, i interface{}) {
		if strings.HasPrefix(name, "_") {
			// in-mem only private metrics, will not dump to influxdb
			return
		}

		name, tags = this.extractTagsFromMetricsName(name)

		switch m := i.(type) {
		case metrics.Counter:
			pts = append(pts, client.Point{
				Measurement: fmt.Sprintf("%s.count", name), // TODO perf
				Fields: map[string]interface{}{
					"value": m.Count(),
				},
				Tags: tags,
				Time: now,
			})

		case metrics.Gauge:
			pts = append(pts, client.Point{
				Measurement: fmt.Sprintf("%s.gauge", name),
				Fields: map[string]interface{}{
					"value": m.Value(),
				},
				Tags: tags,
				Time: now,
			})

		case metrics.GaugeFloat64:
			pts = append(pts, client.Point{
				Measurement: fmt.Sprintf("%s.gauge", name),
				Fields: map[string]interface{}{
					"value": m.Value(),
				},
				Tags: tags,
				Time: now,
			})

		case metrics.Histogram:
			ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			pts = append(pts, client.Point{
				Measurement: fmt.Sprintf("%s.histogram", name),
				Fields: map[string]interface{}{
					"count":    m.Count(),
					"max":      m.Max(),
					"mean":     m.Mean(),
					"min":      m.Min(),
					"stddev":   m.StdDev(),
					"variance": m.Variance(),
					"p50":      ps[0],
					"p75":      ps[1],
					"p95":      ps[2],
					"p99":      ps[3],
					"p999":     ps[4],
					"p9999":    ps[5],
				},
				Tags: tags,
				Time: now,
			})

		case metrics.Timer:
			ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			pts = append(pts, client.Point{
				Measurement: fmt.Sprintf("%s.timer", name),
				Fields: map[string]interface{}{
					"count":    m.Count(),
					"max":      m.Max(),
					"mean":     m.Mean(),
					"min":      m.Min(),
					"stddev":   m.StdDev(),
					"variance": m.Variance(),
					"p50":      ps[0],
					"p75":      ps[1],
					"p95":      ps[2],
					"p99":      ps[3],
					"p999":     ps[4],
					"p9999":    ps[5],
					"m1":       m.Rate1(),
					"m5":       m.Rate5(),
					"m15":      m.Rate15(),
					"meanrate": m.RateMean(),
				},
				Tags: tags,
				Time: now,
			})

		case metrics.Meter:
			pts = append(pts, client.Point{
				Measurement: fmt.Sprintf("%s.meter", name),
				Fields: map[string]interface{}{
					"count": m.Count(),
					"m1":    m.Rate1(),
					"m5":    m.Rate5(),
					"m15":   m.Rate15(),
					"mean":  m.RateMean(),
				},
				Tags: tags,
				Time: now,
			})

		case metrics.Healthcheck:
			// ignored

		}
	})

	return
}

func (this *reporter) writeInfluxDB(pts []client.Point) {
	if this.client == nil {
		log.Warn("influxdb write while connection lost, retry...")

		if err := this.makeClient(); err != nil {
			log.Error("influxdb connect retry: %v", err)
			return
		} else {
			log.Info("influxdb connect retry ok")
		}
	}

	if _, err := this.client.Write(client.BatchPoints{
		Points:   pts,
		Database: this.cf.database,
	}); err != nil {
		log.Error("influxdb write: %v", err)
	}
}
