package gateway

import (
	"fmt"
	uurl "net/url"
	"time"

	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
	"github.com/influxdata/influxdb/client"
)

type reporter struct {
	reg      metrics.Registry
	interval time.Duration
	hostname string

	url      uurl.URL
	database string
	username string
	password string

	client *client.Client
}

// InfluxDB starts a InfluxDB reporter which will post the metrics from the given registry at each d interval.
// CREATE RETENTION POLICY two_hours ON food_data DURATION 2h REPLICATION 1 DEFAULT
// SHOW RETENTION POLICIES ON food_data
// CREATE CONTINUOUS QUERY cq_30m ON food_data BEGIN SELECT mean(website) AS mean_website,mean(phone) AS mean_phone INTO food_data."default".downsampled_orders FROM orders GROUP BY time(30m) END
func InfluxDB(hostname string, r metrics.Registry, interval time.Duration,
	url, database, username, password string) {
	u, err := uurl.Parse(url)
	if err != nil {
		log.Error("unable to parse InfluxDB url %s. err=%v", url, err)
		return
	}

	rep := &reporter{
		reg:      r,
		interval: interval,
		url:      *u,
		database: database,
		hostname: hostname,
		username: username,
		password: password,
	}
	if err := rep.makeClient(); err != nil {
		log.Error("unable to make InfluxDB client. err=%v", err)
		return
	}

	rep.run()
}

func (r *reporter) makeClient() (err error) {
	r.client, err = client.NewClient(client.Config{
		URL:      r.url,
		Username: r.username,
		Password: r.password,
	})

	return
}

func (r *reporter) run() {
	intervalTicker := time.Tick(r.interval)
	//pingTicker := time.Tick(time.Second * 5)
	pingTicker := time.Tick(r.interval / 2)

	for {
		select {
		// TODO on shutdown, flush all metrics

		case <-intervalTicker:
			if err := r.send(); err != nil {
				log.Error("unable to send metrics to InfluxDB. err=%v", err)
			}

		case <-pingTicker:
			_, _, err := r.client.Ping()
			if err != nil {
				log.Error("got error while sending a ping to InfluxDB, trying to recreate client. err=%v", err)

				if err = r.makeClient(); err != nil {
					log.Error("unable to make InfluxDB client. err=%v", err)
				}
			}
		}
	}
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
				"host": r.hostname,
			}
		} else {
			tags = map[string]string{
				"host":  r.hostname,
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
		Database: r.database,
	}

	_, err := r.client.Write(bps)
	return err
}
