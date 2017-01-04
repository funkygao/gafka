package influxdb

import (
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
	"github.com/influxdata/influxdb/client/v2"
)

func init() {
	monitor.RegisterWatcher("influxdb.server", func() monitor.Watcher {
		return &WatchInfluxServer{
			Tick: time.Minute,
		}
	})
}

// WatchInfluxServer continuously pings influxdb server.
type WatchInfluxServer struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	addr string
	cli  client.Client

	influxdbServerAlive metrics.Gauge
}

func (this *WatchInfluxServer) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
	this.addr = ctx.InfluxAddr()

	// warmup
	if this.addr != "" {
		this.influxdbServerAlive = metrics.NewRegisteredGauge("influxdb.alive", nil)
		this.influxdbServerAlive.Update(int64(this.aliveInstances()))
	}

}

func (this *WatchInfluxServer) Run() {
	defer this.Wg.Done()

	if this.addr == "" {
		log.Warn("empty influxdb server addr, quit...")
		return
	}

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	for {
		select {
		case <-this.Stop:
			log.Info("influx.server stopped")
			return

		case <-ticker.C:
			this.influxdbServerAlive.Update(int64(this.aliveInstances()))

		}
	}
}

func (this *WatchInfluxServer) aliveInstances() int {
	var err error
	if this.cli == nil {
		this.cli, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     this.addr,
			Username: "",
			Password: "",
		})
		if err != nil {
			log.Error("influxdb.server: %v", err)
			return 0
		}
		if this.cli == nil {
			log.Error("influxdb.server connected got nil cli")
			return 0
		}
	}

	_, _, err = this.cli.Ping(time.Second * 4)
	if err != nil {
		log.Error("influxdb.server: %v", err)
		return 0
	}

	return 1 // TODO what if multiple influxdb instances?
}
