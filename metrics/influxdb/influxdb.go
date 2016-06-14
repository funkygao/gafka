package influxdb

import (
	"fmt"
	"time"

	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
	"github.com/influxdata/influxdb/client"
)

type reporter struct {
	cf     *config
	reg    metrics.Registry
	client *client.Client
}

// New creates a InfluxDB reporter which will post the metrics from the given registry at each interval.
// CREATE RETENTION POLICY two_hours ON food_data DURATION 2h REPLICATION 1 DEFAULT
// SHOW RETENTION POLICIES ON food_data
// CREATE CONTINUOUS QUERY cq_30m ON food_data BEGIN SELECT mean(website) AS mean_website,mean(phone) AS mean_phone INTO food_data."default".downsampled_orders FROM orders GROUP BY time(30m) END
func New(r metrics.Registry, cf *config) *reporter {
	this := &reporter{
		reg: r,
		cf:  cf,
	}

	return this
}

func (this *reporter) makeClient() (err error) {
	this.client, err = client.NewClient(client.Config{
		URL:      this.cf.url,
		Username: this.cf.username,
		Password: this.cf.password,
	})

	return
}

func (this *reporter) Start() error {
	if err := this.makeClient(); err != nil {
		return err
	}

	intervalTicker := time.Tick(this.cf.interval)
	//pingTicker := time.Tick(time.Second * 5)
	pingTicker := time.Tick(this.cf.interval / 2)

	for {
		select {
		// TODO on shutdown, flush all metrics

		case <-intervalTicker:
			if err := this.send(); err != nil {
				log.Error("unable to send metrics to InfluxDB. err=%v", err)
			}

		case <-pingTicker:
			_, _, err := this.client.Ping()
			if err != nil {
				log.Error("got error while sending a ping to InfluxDB, trying to recreate client. err=%v", err)

				if err = this.makeClient(); err != nil {
					log.Error("unable to make InfluxDB client. err=%v", err)
				}
			}
		}
	}

	return nil
}

func (r *reporter) send() error {
	var pts []client.Point

	var (
		appid, topic, ver string
		tags              map[string]string
	)
	r.reg.Each(func(name string, i interface{}) {
		now := time.Now()
		appid, topic, ver, name = extractFromMetricsName(name)

		if appid == "" {
			tags = map[string]string{
				"host": r.cf.hostname,
			}
		} else {
			tags = map[string]string{
				"host":  r.cf.hostname,
				"appid": appid,
				"topic": topic,
				"ver":   ver,
			}
		}

		switch m := i.(type) {
		case metrics.Counter:
			pts = append(pts, client.Point{
				Measurement: fmt.Sprintf("%s.count", name),
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
		}
	})

	bps := client.BatchPoints{
		Points:   pts,
		Database: r.cf.database,
	}

	_, err := r.client.Write(bps)
	return err
}
