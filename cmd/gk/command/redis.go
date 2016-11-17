package command

import (
	"flag"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/funkygao/Go-Redis"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/termui"
	"github.com/pmylund/sortutil"
	"github.com/ryanuber/columnize"
)

type Redis struct {
	Ui  cli.Ui
	Cmd string

	mu       sync.Mutex
	topInfos []redisTopInfo
}

func (this *Redis) Run(args []string) (exitCode int) {
	var (
		zone   string
		add    string
		list   bool
		byHost bool
		del    string
		top    bool
		ping   bool
	)
	cmdFlags := flag.NewFlagSet("redis", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&add, "add", "", "")
	cmdFlags.BoolVar(&list, "list", true, "")
	cmdFlags.BoolVar(&byHost, "host", false, "")
	cmdFlags.BoolVar(&top, "top", false, "")
	cmdFlags.BoolVar(&ping, "ping", false, "")
	cmdFlags.StringVar(&del, "del", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	if top || ping {
		list = false
	}

	if add != "" {
		host, port, err := net.SplitHostPort(add)
		swallow(err)

		nport, err := strconv.Atoi(port)
		swallow(err)
		zkzone.AddRedis(host, nport)
	} else if del != "" {
		host, port, err := net.SplitHostPort(del)
		swallow(err)

		nport, err := strconv.Atoi(port)
		swallow(err)
		zkzone.DelRedis(host, nport)
	} else {
		if top {
			this.runTop(zkzone)
		} else if ping {
			this.runPing(zkzone)
		} else if list {
			machineMap := make(map[string]struct{})
			var machines []string
			hostPorts := zkzone.AllRedis()
			sort.Strings(hostPorts)
			for _, hp := range hostPorts {
				host, port, _ := net.SplitHostPort(hp)
				ips, _ := net.LookupIP(host)
				if _, present := machineMap[ips[0].String()]; !present {
					machineMap[ips[0].String()] = struct{}{}

					machines = append(machines, ips[0].String())
				}
				if !byHost {
					this.Ui.Output(fmt.Sprintf("%35s %s", host, port))
				}

			}

			if byHost {
				sort.Strings(machines)
				for _, ip := range machines {
					this.Ui.Output(fmt.Sprintf("%20s", ip))
				}
			}

			this.Ui.Output(fmt.Sprintf("Total instances:%d machines:%d", len(hostPorts), len(machines)))
		}
	}

	return
}

type redisTopInfo struct {
	host                       string
	port                       int
	dbsize, ops, rx, tx, conns int64
	latency                    time.Duration
}

func (this *Redis) runTop(zkzone *zk.ZkZone) {
	termui.Init()
	limit := termui.TermHeight() - 3
	termui.Close()
	this.topInfos = make([]redisTopInfo, 0, 100)
	for {
		var wg sync.WaitGroup
		this.topInfos = this.topInfos[:0]
		for _, hostPort := range zkzone.AllRedis() {
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

			wg.Add(1)
			go this.updateRedisInfo(&wg, host, nport)
		}
		wg.Wait()
		refreshScreen()

		sortutil.DescByField(this.topInfos, "ops")
		lines := []string{"Host|Port|dbsize|conns|ops|rx/bps|tx/bps"}

		for i := 0; i < min(limit, len(this.topInfos)); i++ {
			info := this.topInfos[i]
			lines = append(lines, fmt.Sprintf("%s|%d|%s|%s|%s|%s|%s",
				info.host, info.port,
				gofmt.Comma(info.dbsize), gofmt.Comma(info.conns), gofmt.Comma(info.ops),
				gofmt.ByteSize(info.rx*1024/8), gofmt.ByteSize(info.tx*1024/8)))
		}

		this.Ui.Output(columnize.SimpleFormat(lines))

		time.Sleep(time.Second * 5)
	}
}

func (this *Redis) updateRedisInfo(wg *sync.WaitGroup, host string, port int) {
	defer wg.Done()

	spec := redis.DefaultSpec().Host(host).Port(port)
	client, err := redis.NewSynchClientWithSpec(spec)
	if err != nil {
		return
	}
	defer client.Quit()

	infoMap, err := client.Info()
	if err != nil {
		return
	}

	dbSize, _ := client.Dbsize()
	conns, _ := strconv.ParseInt(infoMap["connected_clients"], 10, 64)
	ops, _ := strconv.ParseInt(infoMap["instantaneous_ops_per_sec"], 10, 64)
	rxKbps, _ := strconv.ParseFloat(infoMap["instantaneous_input_kbps"], 64)
	txKbps, _ := strconv.ParseFloat(infoMap["instantaneous_output_kbps"], 64)

	this.mu.Lock()
	this.topInfos = append(this.topInfos, redisTopInfo{
		host:   host,
		port:   port,
		dbsize: dbSize,
		ops:    ops,
		rx:     int64(rxKbps),
		tx:     int64(txKbps),
		conns:  conns,
	})
	this.mu.Unlock()
}

func (this *Redis) runPing(zkzone *zk.ZkZone) {
	var wg sync.WaitGroup
	this.topInfos = make([]redisTopInfo, 0, 100)

	for _, hostPort := range zkzone.AllRedis() {
		host, port, err := net.SplitHostPort(hostPort)
		if err != nil {
			this.Ui.Error(hostPort)
			continue
		}

		nport, err := strconv.Atoi(port)
		if err != nil || nport < 0 {
			this.Ui.Error(hostPort)
			continue
		}

		wg.Add(1)
		go func(wg *sync.WaitGroup, host string, port int) {
			defer wg.Done()

			t0 := time.Now()

			spec := redis.DefaultSpec().Host(host).Port(port)
			client, err := redis.NewSynchClientWithSpec(spec)
			if err != nil {
				this.Ui.Error(err.Error())
			}
			defer client.Quit()

			if err := client.Ping(); err != nil {
				this.Ui.Error(err.Error())
			}

			this.mu.Lock()
			this.topInfos = append(this.topInfos, redisTopInfo{
				host:    host,
				port:    port,
				latency: time.Since(t0),
			})
			this.mu.Unlock()
		}(&wg, host, nport)
	}
	wg.Wait()

	sortutil.AscByField(this.topInfos, "latency")
	lines := []string{"Host|Port|latency"}
	for _, info := range this.topInfos {
		lines = append(lines, fmt.Sprintf("%s|%d|%s", info.host, info.port, info.latency))
	}
	this.Ui.Output(columnize.SimpleFormat(lines))
}

func (*Redis) Synopsis() string {
	return "Monitor redis instances"
}

func (this *Redis) Help() string {
	help := fmt.Sprintf(`
Usage: %s redis [options]

    %s

    -z zone

    -list

    -top
      Monitor all redis instances ops

    -ping
      Ping all redis instances

    -host
      Work with -list, print host instead of redis instance

    -add host:port

    -del host:port

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
