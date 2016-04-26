package command

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/bjtime"
	"github.com/funkygao/golib/color"
)

type Zktop struct {
	Ui  cli.Ui
	Cmd string

	batchMode       bool
	refreshInterval time.Duration
	lastSents       map[string]string
	lastRecvs       map[string]string
}

func (this *Zktop) Run(args []string) (exitCode int) {
	var (
		zone string
	)
	cmdFlags := flag.NewFlagSet("zktop", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.BoolVar(&this.batchMode, "b", false, "")
	cmdFlags.DurationVar(&this.refreshInterval, "i", time.Second*5, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	this.lastRecvs = make(map[string]string)
	this.lastSents = make(map[string]string)

	if this.batchMode {
		this.Ui = &cli.BasicUi{
			Writer:      os.Stdout,
			Reader:      os.Stdin,
			ErrorWriter: os.Stderr,
		}
	}

	for {
		if !this.batchMode {
			refreshScreen()
		}

		if zone == "" {
			forAllSortedZones(func(zkzone *zk.ZkZone) {
				this.displayZoneTop(zkzone)
			})
		} else {
			zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
			this.displayZoneTop(zkzone)
		}

		time.Sleep(this.refreshInterval)
	}

	return
}

func (this *Zktop) displayZoneTop(zkzone *zk.ZkZone) {
	if this.batchMode {
		this.Ui.Output(fmt.Sprintf("%s %s", zkzone.Name(), bjtime.NowBj()))
	} else {
		this.Ui.Output(color.Green(zkzone.Name()))
	}

	header := "VER             SERVER           PORT M  OUTST            RECVD             SENT CONNS  ZNODES LAT(MIN/AVG/MAX)"
	this.Ui.Output(header)

	stats := zkzone.RunZkFourLetterCommand("stat")
	sortedHosts := make([]string, 0, len(stats))
	for hp, _ := range stats {
		sortedHosts = append(sortedHosts, hp)
	}
	sort.Strings(sortedHosts)

	for _, hostPort := range sortedHosts {
		host, port, err := net.SplitHostPort(hostPort)
		if err != nil {
			panic(err)
		}

		stat := this.parsedStat(stats[hostPort])
		if stat.mode == "" {
			if this.batchMode {
				stat.mode = "E"
			} else {
				stat.mode = color.Red("E")
			}
		} else if stat.mode == "L" && !this.batchMode {
			stat.mode = color.Blue(stat.mode)
		}
		var sentQps, recvQps int
		if lastRecv, present := this.lastRecvs[hostPort]; present {
			r1, _ := strconv.Atoi(stat.received)
			r0, _ := strconv.Atoi(lastRecv)
			recvQps = (r1 - r0) / int(this.refreshInterval.Seconds())

			s1, _ := strconv.Atoi(stat.sent)
			s0, _ := strconv.Atoi(this.lastSents[hostPort])
			sentQps = (s1 - s0) / int(this.refreshInterval.Seconds())
		}
		this.Ui.Output(fmt.Sprintf("%-15s %-15s %5s %1s %6s %16s %16s %5s %7s %s",
			stat.ver,                                     // 15
			host,                                         // 15
			port,                                         // 5
			stat.mode,                                    // 1
			stat.outstanding,                             // 6
			fmt.Sprintf("%s/%d", stat.received, recvQps), // 16
			fmt.Sprintf("%s/%d", stat.sent, sentQps),     // 16
			stat.connections,                             // 5
			stat.znodes,                                  // 7
			stat.latency,
		))

		this.lastRecvs[hostPort] = stat.received
		this.lastSents[hostPort] = stat.sent
	}
}

type zkStat struct {
	ver            string
	latency        string
	connections    string
	outstanding    string
	mode           string
	znodes         string
	received, sent string
}

func (this *Zktop) parsedStat(s string) (stat zkStat) {
	lines := strings.Split(s, "\n")
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, "Zookeeper version:"):
			p := strings.SplitN(l, ":", 2)
			p = strings.SplitN(p[1], ",", 2)
			stat.ver = strings.TrimSpace(p[0])

		case strings.HasPrefix(l, "Latency"):
			stat.latency = this.extractStatValue(l)

		case strings.HasPrefix(l, "Sent"):
			stat.sent = this.extractStatValue(l)

		case strings.HasPrefix(l, "Received"):
			stat.received = this.extractStatValue(l)

		case strings.HasPrefix(l, "Connections"):
			stat.connections = this.extractStatValue(l)

		case strings.HasPrefix(l, "Mode"):
			stat.mode = strings.ToUpper(this.extractStatValue(l)[:1])

		case strings.HasPrefix(l, "Node count"):
			stat.znodes = this.extractStatValue(l)

		case strings.HasPrefix(l, "Outstanding"):
			stat.outstanding = this.extractStatValue(l)

		}
	}
	return
}

func (this *Zktop) extractStatValue(l string) string {
	p := strings.SplitN(l, ":", 2)
	return strings.TrimSpace(p[1])
}

func (*Zktop) Synopsis() string {
	return "Unix “top” like utility for ZooKeeper"
}

func (this *Zktop) Help() string {
	help := fmt.Sprintf(`
Usage: %s zktop [options]

    Unix “top” like utility for ZooKeeper

Options:

    -z zone   

    -i interval
      Refresh interval in seconds.
      e,g. 5s

    -b
      Batch mode operation.

`, this.Cmd)
	return strings.TrimSpace(help)
}
