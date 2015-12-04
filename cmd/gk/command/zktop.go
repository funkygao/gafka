package command

import (
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Zktop struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Zktop) Run(args []string) (exitCode int) {
	var zone string
	cmdFlags := flag.NewFlagSet("zktop", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	for {
		refreshScreen()

		if zone == "" {
			forSortedZones(func(zkzone *zk.ZkZone) {
				this.displayZoneTop(zkzone)
			})
		} else {
			zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
			this.displayZoneTop(zkzone)
		}

		time.Sleep(time.Second * 3)
	}

	return
}

func (this *Zktop) displayZoneTop(zkzone *zk.ZkZone) {
	this.Ui.Output(color.Green(zkzone.Name()))
	header := "SERVER           PORT M      OUTST        RECVD         SENT CONNS ZNODES LAT(MIN/AVG/MAX)"
	this.Ui.Output(header)

	for hostPort, lines := range zkzone.RunZkFourLetterCommand("stat") {
		host, port, err := net.SplitHostPort(hostPort)
		if err != nil {
			panic(err)
		}

		stat := this.parsedStat(lines)
		this.Ui.Output(fmt.Sprintf("%-15s %5s %1s %10s %12s %12s %5s %6s %s",
			host, port,
			stat.mode,
			stat.outstanding,
			stat.received,
			stat.sent,
			stat.connections,
			stat.znodes,
			stat.latency,
		))
	}
}

type zkStat struct {
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

`, this.Cmd)
	return strings.TrimSpace(help)
}
