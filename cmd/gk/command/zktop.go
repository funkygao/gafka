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

		forAllSortedZones(func(zkzone *zk.ZkZone) {
			if !patternMatched(zkzone.Name(), zone) {
				return
			}

			this.displayZoneTop(zkzone)
		})

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

		stat := zk.ParseStatResult(stats[hostPort])
		if stat.Mode == "" {
			if this.batchMode {
				stat.Mode = "E"
			} else {
				stat.Mode = color.Red("E")
			}
		} else if stat.Mode == "L" && !this.batchMode {
			stat.Mode = color.Blue(stat.Mode)
		}
		var sentQps, recvQps int
		if lastRecv, present := this.lastRecvs[hostPort]; present {
			r1, _ := strconv.Atoi(stat.Received)
			r0, _ := strconv.Atoi(lastRecv)
			recvQps = (r1 - r0) / int(this.refreshInterval.Seconds())

			s1, _ := strconv.Atoi(stat.Sent)
			s0, _ := strconv.Atoi(this.lastSents[hostPort])
			sentQps = (s1 - s0) / int(this.refreshInterval.Seconds())
		}
		this.Ui.Output(fmt.Sprintf("%-15s %-15s %5s %1s %6s %16s %16s %5s %7s %s",
			stat.Version,                                 // 15
			host,                                         // 15
			port,                                         // 5
			stat.Mode,                                    // 1
			stat.Outstanding,                             // 6
			fmt.Sprintf("%s/%d", stat.Received, recvQps), // 16
			fmt.Sprintf("%s/%d", stat.Sent, sentQps),     // 16
			stat.Connections,                             // 5
			stat.Znodes,                                  // 7
			stat.Latency,
		))

		this.lastRecvs[hostPort] = stat.Received
		this.lastSents[hostPort] = stat.Sent
	}
}

func (*Zktop) Synopsis() string {
	return "Unix “top” like utility for ZooKeeper"
}

func (this *Zktop) Help() string {
	help := fmt.Sprintf(`
Usage: %s zktop [options]

    %s

Options:

    -z zone pattern

    -i interval
      Refresh interval in seconds.
      e,g. 5s

    -b
      Batch mode operation.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
