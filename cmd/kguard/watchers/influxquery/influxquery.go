package influxquery

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
	"github.com/influxdata/influxdb/client/v2"
)

var errInfluxResult = errors.New("bad reply from influx query result")

func init() {
	monitor.RegisterWatcher("influx.query", func() monitor.Watcher {
		return &WatchInfluxQuery{
			Tick: time.Minute,
		}
	})
}

// WatchInfluxQuery continuously query InfluxDB for major metrics.
//
// kateway itself will report metrics to influxdb, but there are
// several kateway instances in a cluster, we need kguard to aggregate them.
type WatchInfluxQuery struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	addr string
	db   string
	cli  client.Client
}

func (this *WatchInfluxQuery) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
	this.addr = ctx.InfluxAddr()
	this.db = ctx.InfluxDB()
}

func (this *WatchInfluxQuery) Run() {
	defer this.Wg.Done()

	if this.addr == "" || this.db == "" {
		log.Warn("empty addr or db, quit...")
		return
	}

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	pubLatency := metrics.NewRegisteredGauge("_pub.latency.99", nil) // private metric name
	katewayMaxHeap := metrics.NewRegisteredGauge("_kateway.max_heap", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("influx.query stopped")
			return

		case <-ticker.C:
			p99, err := this.pubLatency()
			if err != nil {
				log.Error("influx.query[_pub.latency.99]: %v", err)
			} else {
				pubLatency.Update(int64(p99))
			}

			maxHeap, err := this.katewayMaxHeapSize()
			if err != nil {
				log.Error("influx.query[_kateway.max_heap]: %v", err)
			} else {
				katewayMaxHeap.Update(int64(maxHeap))
			}
		}
	}
}

func (this *WatchInfluxQuery) katewayMaxHeapSize() (float64, error) {
	res, err := this.queryDB(`SELECT max("value") FROM "runtime.MemStats.HeapSys.gauge" WHERE time > now() - 1m`)
	if err != nil {
		return 0, err
	}

	if len(res) > 0 && len(res[0].Series) >= 1 && len(res[0].Series[0].Values) > 0 && len(res[0].Series[0].Values[0]) > 1 {
		maxHeapSysInBytes := res[0].Series[0].Values[0][1].(json.Number)
		return maxHeapSysInBytes.Float64()
	}

	return 0, errInfluxResult
}

func (this *WatchInfluxQuery) pubLatency() (float64, error) {
	res, err := this.queryDB(`SELECT mean("p99") FROM "pub.latency.histogram" where time > now() - 1m`)
	if err != nil {
		return 0, err
	}

	// res = []client.Result{
	//    client.Result{
	//      Series:[]models.Row{
	//        models.Row{
	//          Name:"pub.latency.histogram",
	//          Tags:map[string]string(nil),
	//          Columns:[]string{"time", "mean"},
	//          Values:[][]interface {}{[]interface {}{"2016-06-25T09:10:49.756661374Z", "0.4"}},
	//          Err:error(nil)
	//        }
	//      },
	//      Err:""
	//    }
	//  }
	if len(res) > 0 && len(res[0].Series) >= 1 && len(res[0].Series[0].Values) > 0 && len(res[0].Series[0].Values[0]) > 1 {
		p99 := res[0].Series[0].Values[0][1].(json.Number) // values[0][0] is "time"
		return p99.Float64()
	}

	return 0., errInfluxResult
}

// queryDB convenience function to query the database
func (this *WatchInfluxQuery) queryDB(cmd string) (res []client.Result, err error) {
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
