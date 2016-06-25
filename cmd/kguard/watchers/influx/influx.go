package influx

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	log "github.com/funkygao/log4go"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/rcrowley/go-metrics"
)

func init() {
	monitor.RegisterWatcher("influx.query", func() monitor.Watcher {
		return &WatchInfluxDB{
			Tick: time.Minute,
		}
	})
}

// WatchInfluxDB continuously query InfluxDB for major metrics.
//
// kateway itself will report metrics to influxdb, but there are
// several kateway instances in a cluster, we need kguard to aggregate them.
type WatchInfluxDB struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	addr string
	db   string
	cli  client.Client
}

func (this *WatchInfluxDB) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.WaitGroup()
	this.addr = ctx.InfluxAddr()
	this.db = ctx.InfluxDB()
}

func (this *WatchInfluxDB) Run() {
	defer this.Wg.Done()

	if this.addr == "" || this.db == "" {
		log.Warn("empty addr or db, quit...")
		return
	}

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	// SELECT mean("p99") FROM "pub.latency.histogram" WHERE $timeFilter GROUP BY time(10s)
	// DB: pubsub
	pubLatency := metrics.NewRegisteredGauge("_pub.latency.99", nil) // private metric name

	for {
		select {
		case <-this.Stop:
			log.Info("influx.query stopped")
			return

		case <-ticker.C:
			p99, err := this.pubLatency()
			if err != nil {
				log.Error(err.Error())
			} else {
				pubLatency.Update(int64(p99))
			}
		}
	}
}

func (this *WatchInfluxDB) pubLatency() (float64, error) {
	res, err := this.queryDB(`SELECT mean("p99") FROM "pub.latency.histogram" where time > now() - 1m`)
	if err != nil {
		return 0, err
	}

	p99 := res[0].Series[0].Values[0][1].(json.Number) // values[0][0] is "time"
	return p99.Float64()
}

// queryDB convenience function to query the database
func (this *WatchInfluxDB) queryDB(cmd string) (res []client.Result, err error) {
	if this.cli == nil {
		this.cli, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     this.addr,
			Username: "",
			Password: "",
		})
		if err != nil {
			return
		}
	}

	q := client.Query{
		Command:  cmd,
		Database: "pubsub", // FIXME for historical mistakes, we are using another db, not this.db
	}
	if response, err := this.cli.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}

	return res, nil
}
