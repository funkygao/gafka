package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

const (
	zk_cmd_help = "Accepted commands include: watchers, env, conns, conf, stat, dump"
)

type Zookeeper struct {
	Ui  cli.Ui
	Cmd string
	flw string // zk four letter word command
}

func (this *Zookeeper) Run(args []string) (exitCode int) {
	var (
		zone string
	)
	cmdFlags := flag.NewFlagSet("zookeeper", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&this.flw, "cmd", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).require("-cmd").invalid(args) {
		return 2
	}

	if zone != "" {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		this.printZkStats(zkzone)

		return
	}

	// print all by default
	forAllZones(func(zkzone *zk.ZkZone) {
		this.printZkStats(zkzone)
	})

	return
}

func (this *Zookeeper) printZkStats(zkzone *zk.ZkZone) {
	switch this.flw {
	case "stat":
		for server, line := range zkzone.ZkStats() {
			this.Ui.Output(fmt.Sprintf("\t%s\n%s", color.Green(server), line))
		}

	case "watchers":
		for server, line := range zkzone.ZkWatchers() {
			this.Ui.Output(fmt.Sprintf("\t%s\n%s", color.Green(server), line))
		}

	case "dump":
		for server, line := range zkzone.ZkDump() {
			this.Ui.Output(fmt.Sprintf("\t%s\n%s", color.Green(server), line))
		}

	case "conf":
		for server, line := range zkzone.ZkConf() {
			this.Ui.Output(fmt.Sprintf("\t%s\n%s", color.Green(server), line))
		}

	case "env":
		for server, line := range zkzone.ZkEnv() {
			this.Ui.Output(fmt.Sprintf("\t%s\n%s", color.Green(server), line))
		}

	case "conns":
		for server, line := range zkzone.ZkConnections() {
			this.Ui.Output(fmt.Sprintf("\t%s\n%s", color.Green(server), line))
		}

	default:
		this.Ui.Error("invalid command")
		this.Ui.Info(zk_cmd_help)
	}

}

func (*Zookeeper) Synopsis() string {
	return "Display zone Zookeeper status"
}

func (this *Zookeeper) Help() string {
	help := fmt.Sprintf(`
Usage: %s zookeeper [options]

    Display zone Zookeeper status

Options:

    -z zone

    -c command
      %s
`, this.Cmd, zk_cmd_help)
	return strings.TrimSpace(help)
}
