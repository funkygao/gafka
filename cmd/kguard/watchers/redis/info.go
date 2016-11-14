package zk

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/Go-Redis"
	"github.com/funkygao/gafka/cmd/kguard/monitor"
	"github.com/funkygao/gafka/telemetry"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/go-metrics"
	log "github.com/funkygao/log4go"
)

func init() {
	monitor.RegisterWatcher("redis.info", func() monitor.Watcher {
		return &WatchRedisInfo{
			Tick: time.Minute,
		}
	})
}

// WatchRedisInfo watches registered redis instances with redis 'info' command.
type WatchRedisInfo struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	deadInstance, syncPartial metrics.Counter
	conns                     map[string]metrics.Gauge
	blocked                   map[string]metrics.Gauge
	usedMem                   map[string]metrics.Gauge
	ops                       map[string]metrics.Gauge
	rejected                  map[string]metrics.Gauge
	rxKbps                    map[string]metrics.Gauge
	txKbps                    map[string]metrics.Gauge
	expiredKeys               map[string]metrics.Gauge
}

func (this *WatchRedisInfo) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchRedisInfo) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	this.deadInstance = metrics.NewRegisteredCounter("redis.dead", nil)
	this.syncPartial = metrics.NewRegisteredCounter("redis.sync.partial", nil)
	this.conns = make(map[string]metrics.Gauge, 10)
	this.blocked = make(map[string]metrics.Gauge, 10)
	this.usedMem = make(map[string]metrics.Gauge, 10)
	this.ops = make(map[string]metrics.Gauge, 10)
	this.rejected = make(map[string]metrics.Gauge, 10)
	this.rxKbps = make(map[string]metrics.Gauge, 10)
	this.txKbps = make(map[string]metrics.Gauge, 10)
	this.expiredKeys = make(map[string]metrics.Gauge, 10)

	for {
		select {
		case <-this.Stop:
			log.Info("redis.info stopped")
			return

		case <-ticker.C:
			var wg sync.WaitGroup
			for _, hostPort := range this.Zkzone.AllRedis() {
				host, port, err := net.SplitHostPort(hostPort)
				if err != nil {
					log.Error("invalid redis instance: %s", hostPort)
					continue
				}

				nport, err := strconv.Atoi(port)
				if err != nil || nport < 0 {
					log.Error("invalid redis instance: %s", hostPort)
					continue
				}

				// TODO ver=role(master|slave)
				tag := telemetry.Tag(strings.Replace(host, ".", "_", -1), port, "v1")
				if _, present := this.conns[tag]; !present {
					this.conns[tag] = metrics.NewRegisteredGauge(tag+"redis.conns", nil)              // connected_clients
					this.blocked[tag] = metrics.NewRegisteredGauge(tag+"redis.blocked", nil)          // blocked_clients
					this.usedMem[tag] = metrics.NewRegisteredGauge(tag+"redis.mem.used", nil)         // used_memory
					this.ops[tag] = metrics.NewRegisteredGauge(tag+"redis.ops", nil)                  // instantaneous_ops_per_sec
					this.rejected[tag] = metrics.NewRegisteredGauge(tag+"redis.rejected", nil)        // rejected_connections
					this.rxKbps[tag] = metrics.NewRegisteredGauge(tag+"redis.rx.kbps", nil)           // instantaneous_input_kbps
					this.txKbps[tag] = metrics.NewRegisteredGauge(tag+"redis.tx.kbps", nil)           // instantaneous_output_kbps
					this.expiredKeys[tag] = metrics.NewRegisteredGauge(tag+"redis.expired.keys", nil) // expired_keys
				}

				wg.Add(1)
				go this.updateRedisInfo(&wg, host, nport, tag)
			}

			wg.Wait()

		}
	}
}

func (this *WatchRedisInfo) updateRedisInfo(wg *sync.WaitGroup, host string, port int, tag string) {
	defer wg.Done()

	spec := redis.DefaultSpec().Host(host).Port(port)
	client, err := redis.NewSynchClientWithSpec(spec)
	if err != nil {
		log.Error("redis[%s:%d]: %v", host, port, err)
		this.deadInstance.Inc(1)
		return
	}
	defer client.Quit()

	infoMap, err := client.Info()
	if err != nil {
		log.Error("redis[%s:%d] info: %v", host, port, err)
		this.deadInstance.Inc(1)
		return
	}

	conns, _ := strconv.ParseInt(infoMap["connected_clients"], 10, 64)
	blocked, _ := strconv.ParseInt(infoMap["blocked_clients"], 10, 64)
	mem, _ := strconv.ParseInt(infoMap["used_memory"], 10, 64)
	ops, _ := strconv.ParseInt(infoMap["instantaneous_ops_per_sec"], 10, 64)
	rejected, _ := strconv.ParseInt(infoMap["rejected_connections"], 10, 64)
	syncPartial, _ := strconv.ParseInt(infoMap["sync_partial_err"], 10, 64)
	rxKbps, _ := strconv.ParseInt(infoMap["instantaneous_input_kbps"], 10, 64)
	txKbps, _ := strconv.ParseInt(infoMap["instantaneous_output_kbps"], 10, 64)
	expiredKeys, _ := strconv.ParseInt(infoMap["expired_keys"], 10, 64)

	this.syncPartial.Inc(syncPartial)

	this.conns[tag].Update(conns)
	this.blocked[tag].Update(blocked)
	this.usedMem[tag].Update(mem)
	this.ops[tag].Update(ops)
	this.rejected[tag].Update(rejected)
	this.rxKbps[tag].Update(rxKbps)
	this.txKbps[tag].Update(txKbps)
	this.expiredKeys[tag].Update(expiredKeys)
}
