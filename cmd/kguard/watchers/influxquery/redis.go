package influxquery

import (
	"encoding/json"
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

	highLoadRedisInstances map[string]struct{} // key is port
}

func (this *WatchRedis) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()

	this.addr = ctx.InfluxAddr()
	this.db = "redis"
	this.highLoadRedisInstances = make(map[string]struct{})
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
			redisN, err := this.redisTopCpu(75)
			if err != nil {
				log.Error("redis.query[redis.top]: %v", err)
			} else {
				redisHighLoad.Update(int64(redisN))
			}
		}
	}
}

func (this *WatchRedis) redisTopCpu(usageThreshold int) (int, error) {
	res, err := queryInfluxDB(this.addr, this.db,
		fmt.Sprintf(`SELECT cpu, port FROM "top" WHERE time > now() - 1m AND cpu >=%d`, usageThreshold))
	if err != nil {
		return 0, err
	}

	newHighLoad := make(map[string]struct{})
	for _, row := range res {
		for _, x := range row.Series {
			for _, val := range x.Values {
				// val[0] is time
				cpu, _ := val[1].(json.Number).Float64()
				port := val[2].(string)
				log.Warn("redis:%s using too much cpu %.1f", port, cpu)
				newHighLoad[port] = struct{}{}
			}
		}
	}

	n := 0
	for port := range newHighLoad {
		if _, present := this.highLoadRedisInstances[port]; present {
			// high cpu across at least 2 rounds
			n++
		} else {
			// a new high load instance found
			this.highLoadRedisInstances[port] = struct{}{}
		}
	}

	for port := range this.highLoadRedisInstances {
		if _, present := newHighLoad[port]; !present {
			log.Info("redis:%s resumes to normal", port)
			delete(this.highLoadRedisInstances, port)
		}
	}

	return n, err
}
