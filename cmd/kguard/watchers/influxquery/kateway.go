package influxquery

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("influx.kateway", func() monitor.Watcher {
		return &WatchInfluxKateway{
			Tick: time.Minute,
		}
	})
}

// WatchInfluxKateway continuously query InfluxDB for kateway aggregated metrics.
type WatchInfluxKateway struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	addr string
	db   string
}

func (this *WatchInfluxKateway) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()

	this.addr = ctx.InfluxAddr()
	this.db = "pubsub"
}

func (this *WatchInfluxKateway) Run() {
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
			log.Info("influx.kateway stopped")
			return

		case <-ticker.C:
			p99, err := this.pubLatency()
			if err != nil {
				log.Error("influx.kateway[_pub.latency.99]: %v", err)
			} else {
				pubLatency.Update(int64(p99))
			}

			maxHeap, err := this.katewayMaxHeapSize()
			if err != nil {
				log.Error("influx.kateway[_kateway.max_heap]: %v", err)
			} else {
				katewayMaxHeap.Update(int64(maxHeap))
			}
		}
	}
}

func (this *WatchInfluxKateway) katewayMaxHeapSize() (float64, error) {
	res, err := queryInfluxDB(this.addr, this.db, `SELECT max("value") FROM "runtime.MemStats.HeapSys.gauge" WHERE time > now() - 1m`)
	if err != nil {
		return 0, err
	}

	if len(res) > 0 && len(res[0].Series) >= 1 && len(res[0].Series[0].Values) > 0 && len(res[0].Series[0].Values[0]) > 1 {
		maxHeapSysInBytes := res[0].Series[0].Values[0][1].(json.Number)
		return maxHeapSysInBytes.Float64()
	}

	return 0, errInfluxResult
}

func (this *WatchInfluxKateway) pubLatency() (float64, error) {
	res, err := queryInfluxDB(this.addr, this.db, `SELECT mean("p99") FROM "pub.latency.histogram" where time > now() - 1m`)
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
