package redis

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
	monitor.RegisterWatcher("redis.slowlog", func() monitor.Watcher {
		return &WatchSlowlog{
			Tick: time.Minute,
		}
	})
}

// WatchSlowlog watches registered redis instances with redis 'slowlog' command.
type WatchSlowlog struct {
	Zkzone *zk.ZkZone
	Stop   <-chan struct{}
	Tick   time.Duration
	Wg     *sync.WaitGroup

	mu    sync.Mutex
	slows map[string]metrics.Gauge
}

func (this *WatchSlowlog) Init(ctx monitor.Context) {
	this.Zkzone = ctx.ZkZone()
	this.Stop = ctx.StopChan()
	this.Wg = ctx.Inflight()
}

func (this *WatchSlowlog) Run() {
	defer this.Wg.Done()

	ticker := time.NewTicker(this.Tick)
	defer ticker.Stop()

	this.slows = make(map[string]metrics.Gauge, 10)

	for {
		select {
		case <-this.Stop:
			log.Info("redis.slowlog stopped")
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

				var ip string
				ips, err := net.LookupIP(host) // host in ip form is also ok e,g. 10.1.1.1
				if err != nil {
					log.Error("redis host[%s] ip: %v", host, err)
				} else if len(ips) > 0 {
					ip = ips[0].String()
				}

				tag := telemetry.Tag(strings.Replace(host, ".", "_", -1), port, ip)

				wg.Add(1)
				go this.updateRedisSlowlog(&wg, host, nport, tag)
			}

			wg.Wait()
		}
	}
}

func (this *WatchSlowlog) updateRedisSlowlog(wg *sync.WaitGroup, host string, port int, tag string) {
	defer wg.Done()

	spec := redis.DefaultSpec().Host(host).Port(port)
	client, err := redis.NewSynchClientWithSpec(spec)
	if err != nil {
		log.Error("redis[%s:%d]: %v", host, port, err)
		return
	}
	defer client.Quit()

	n, err := client.SlowlogLen()
	if err != nil {
		log.Error("redis[%s:%d]: %v", host, port, err)
		return
	}

	if n == 0 {
		return
	}

	this.mu.Lock()
	this.slows[tag] = metrics.NewRegisteredGauge(tag+"redis.slowlog", nil)
	this.slows[tag].Update(n)
	this.mu.Unlock()
}
