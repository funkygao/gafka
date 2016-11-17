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
	"github.com/funkygao/go-metrics"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	log "github.com/funkygao/log4go"
	"github.com/funkygao/termui"
	"github.com/mattn/go-runewidth"
	"github.com/nsf/termbox-go"
	"github.com/pmylund/sortutil"
	"github.com/ryanuber/columnize"
)

type Redis struct {
	Ui  cli.Ui
	Cmd string

	mu       sync.Mutex
	topInfos []redisTopInfo

	quit           chan struct{}
	rows           int
	topOrderAsc    bool
	topOrderColIdx int
	topOrderCols   []string
}

func (this *Redis) Run(args []string) (exitCode int) {
	var (
		zone        string
		add         string
		list        bool
		byHost      int
		del         string
		top         bool
		topInterval time.Duration
		ping        bool
	)
	cmdFlags := flag.NewFlagSet("redis", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&add, "add", "", "")
	cmdFlags.BoolVar(&list, "list", true, "")
	cmdFlags.IntVar(&byHost, "host", 0, "")
	cmdFlags.BoolVar(&top, "top", false, "")
	cmdFlags.DurationVar(&topInterval, "sleep", time.Second*10, "")
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
			this.quit = make(chan struct{})
			this.topOrderAsc = false
			this.topOrderColIdx = 2 // ops by default
			this.topOrderCols = []string{"dbsize", "conns", "ops", "rx", "tx"}
			this.runTop(zkzone, topInterval)
		} else if ping {
			this.runPing(zkzone)
		} else if list {
			machineMap := make(map[string]struct{})
			machinePortMap := make(map[string][]string)
			var machines []string
			hostPorts := zkzone.AllRedis()
			sort.Strings(hostPorts)
			for _, hp := range hostPorts {
				host, port, _ := net.SplitHostPort(hp)
				ips, _ := net.LookupIP(host)
				ip := ips[0].String()
				if _, present := machineMap[ip]; !present {
					machineMap[ip] = struct{}{}
					machinePortMap[ip] = make([]string, 0)

					machines = append(machines, ip)
				}

				if byHost == 0 {
					this.Ui.Output(fmt.Sprintf("%35s %s", host, port))
				} else {
					machinePortMap[ip] = append(machinePortMap[ip], port)
				}

			}

			if byHost > 0 {
				sort.Strings(machines)
				for _, ip := range machines {
					sort.Strings(machinePortMap[ip])
					this.Ui.Info(fmt.Sprintf("%20s %2d ports", ip, len(machinePortMap[ip])))
					if byHost > 1 {
						this.Ui.Output(fmt.Sprintf("%+v", machinePortMap[ip]))
					}

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
	t0                         time.Time
	latency                    time.Duration
}

func (this *Redis) runTop(zkzone *zk.ZkZone, interval time.Duration) {
	termui.Init()
	this.rows = termui.TermHeight() - 4
	defer termui.Close()

	termbox.SetInputMode(termbox.InputEsc)
	eventChan := make(chan termbox.Event, 16)
	go this.handleEvents(eventChan)
	go func() {
		for {
			ev := termbox.PollEvent()
			eventChan <- ev
		}
	}()

	this.topInfos = make([]redisTopInfo, 0, 100)
	tick := time.NewTicker(interval)
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

		this.render()

		select {
		case <-tick.C:

		case <-this.quit:
			return
		}
	}
}

func (this *Redis) handleEvents(eventChan chan termbox.Event) {
	for ev := range eventChan {
		switch ev.Type {
		case termbox.EventKey:
			switch ev.Key {
			case termbox.KeyEsc:
				close(this.quit)
				return

			case termbox.KeyArrowUp:
				this.topOrderAsc = true
				this.render()

			case termbox.KeyArrowDown:
				this.topOrderAsc = false
				this.render()

			case termbox.KeyArrowLeft:
				this.topOrderColIdx--
				if this.topOrderColIdx < 0 {
					this.topOrderColIdx += len(this.topOrderCols)
				}
				this.render()

			case termbox.KeyArrowRight:
				this.topOrderColIdx++
				if this.topOrderColIdx >= len(this.topOrderCols) {
					this.topOrderColIdx -= len(this.topOrderCols)
				}
				this.render()
			}

			switch ev.Ch {
			case 'q':
				close(this.quit)
				return
			}

		}
	}
}

func (this *Redis) drawRow(row string, y int, fg, bg termbox.Attribute) {
	x := 0
	tuples := strings.SplitN(row, "|", 7)
	row = fmt.Sprintf("%30s %7s %10s %10s %10s %10s %10s",
		tuples[0], tuples[1], tuples[2], tuples[3], tuples[4],
		tuples[5], tuples[6])
	for _, r := range row {
		termbox.SetCell(x, y, r, fg, bg)
		// wide string must be considered
		w := runewidth.RuneWidth(r)
		if w == 0 || (w == 2 && runewidth.IsAmbiguousWidth(r)) {
			w = 1
		}
		x += w
	}

}

func (this *Redis) selectedCol() string {
	return this.topOrderCols[this.topOrderColIdx]
}

func (this *Redis) render() {
	termbox.Clear(termbox.ColorDefault, termbox.ColorDefault)

	if this.topOrderAsc {
		sortutil.AscByField(this.topInfos, this.topOrderCols[this.topOrderColIdx])
	} else {
		sortutil.DescByField(this.topInfos, this.topOrderCols[this.topOrderColIdx])
	}
	sortCols := make([]string, len(this.topOrderCols))
	copy(sortCols, this.topOrderCols)
	for i, col := range sortCols {
		if col == this.selectedCol() {
			if this.topOrderAsc {
				sortCols[i] += ">"
			} else {
				sortCols[i] += "<"
			}
		}
	}
	lines := []string{fmt.Sprintf("Host|Port|%s", strings.Join(sortCols, "|"))}

	var (
		sumDbsize, sumConns, sumOps, sumRx, sumTx int64
	)
	for i := 0; i < min(this.rows, len(this.topInfos)); i++ {
		info := this.topInfos[i]
		lines = append(lines, fmt.Sprintf("%s|%d|%s|%s|%s|%s|%s",
			info.host, info.port,
			gofmt.Comma(info.dbsize), gofmt.Comma(info.conns), gofmt.Comma(info.ops),
			gofmt.ByteSize(info.rx*1024/8), gofmt.ByteSize(info.tx*1024/8)))

		sumDbsize += info.dbsize
		sumConns += info.conns
		sumOps += info.ops
		sumRx += info.rx * 1024 / 8
		sumTx += info.tx * 1024 / 8
	}
	lines = append(lines, fmt.Sprintf("-TOTAL-|-%d-|%s|%s|%s|%s|%s",
		len(this.topInfos),
		gofmt.Comma(sumDbsize), gofmt.Comma(sumConns), gofmt.Comma(sumOps),
		gofmt.ByteSize(sumRx), gofmt.ByteSize(sumTx)))

	for row, line := range lines {
		if row == 0 {
			this.drawRow(line, row, termbox.ColorDefault, termbox.ColorCyan)
		} else {
			this.drawRow(line, row, termbox.ColorDefault, termbox.ColorDefault)
		}
	}

	termbox.Flush()
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
	allRedis := zkzone.AllRedis()
	this.topInfos = make([]redisTopInfo, 0, len(allRedis))

	for _, hostPort := range allRedis {
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
				this.Ui.Error(fmt.Sprintf("[%s:%d] %v", host, port, err))
				return
			}
			defer client.Quit()

			if err := client.Ping(); err != nil {
				this.Ui.Error(fmt.Sprintf("[%s:%d] %v", host, port, err))
				return
			}

			latency := time.Since(t0)

			this.mu.Lock()
			this.topInfos = append(this.topInfos, redisTopInfo{
				host:    host,
				port:    port,
				t0:      t0,
				latency: latency,
			})
			this.mu.Unlock()
		}(&wg, host, nport)
	}
	wg.Wait()

	latency := metrics.NewRegisteredHistogram("redis.latency", metrics.DefaultRegistry, metrics.NewExpDecaySample(1028, 0.015))

	sortutil.AscByField(this.topInfos, "latency")
	lines := []string{"Host|Port|At|latency"}
	for _, info := range this.topInfos {
		latency.Update(info.latency.Nanoseconds() / 1e6)

		lines = append(lines, fmt.Sprintf("%s|%d|%s|%s",
			info.host, info.port, info.t0, info.latency))
	}
	this.Ui.Output(columnize.SimpleFormat(lines))

	// summary
	ps := latency.Percentiles([]float64{0.90, 0.95, 0.99, 0.999})
	this.Ui.Info(fmt.Sprintf("N:%d Min:%dms Max:%dms Mean:%.1fms 90%%:%.1fms 95%%:%.1fms 99%%:%.1fms",
		latency.Count(), latency.Min(), latency.Max(), latency.Mean(), ps[0], ps[1], ps[2]))
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

    -host 1|2
      Work with -list, print host instead of redis instance
      1: only display host
      2: 0 + port info

    -top
      Monitor all redis instances ops

    -sleep interval
      Sleep between -top refreshing screen. Defaults 10s
      e,g 10s

    -ping
      Ping all redis instances
    
    -add host:port

    -del host:port

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
