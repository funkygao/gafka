package influxquery

import (
	"fmt"
	"sync"
	"time"

	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("redis.query", func() monitor.Watcher {
		return &WatchRedis{
			Tick: time.Minute,
		}
	})
}

type WatchRedis struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	addr string
	db   string
}

func (this *WatchRedis) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()

	this.addr = ctx.InfluxAddr()
	this.db = "redis"
}

func (this *WatchRedis) Run() {
	defer this.Wg.Done()

	if this.addr == "" || this.db == "" {
		log.Warn("empty addr or db, quit...")
		return
	}

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	redisHighLoad := metrics.NewRegisteredGauge("redis.highload", nil)
	for {
		select {
		case <-this.Stop:
			log.Info("redis.query stopped")
			return

		case <-ticker.C:
			redisN, err := this.redisTopCpu(60)
			if err != nil {
				log.Error("redis.query[redis.top]: %v", err)
			} else {
				redisHighLoad.Update(int64(redisN))
			}
		}
	}
}

func (this *WatchRedis) redisTopCpu(usageThreshold int) (int, error) {
	// TODO group by host:port
	res, err := queryInfluxDB(this.addr, this.db,
		fmt.Sprintf(`SELECT cpu FROM "top" WHERE time > now() - 1m AND cpu >=%d`, usageThreshold))
	if err != nil {
		return 0, err
	}
	if len(res) > 0 {
		n := len(res[0].Series)
		if n > 0 {
			log.Warn("%d redis instances using too much cpu", n)
		}

		return n, nil
	}

	return 0, errInfluxResult
}
