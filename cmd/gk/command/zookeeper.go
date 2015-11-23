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

type Zookeeper struct {
	Ui      cli.Ui
	Cmd     string
	flw     string // zk four letter word command
	cmdHelp string
}

func (this *Zookeeper) Run(args []string) (exitCode int) {
	var (
		zone string
	)
	cmdFlags := flag.NewFlagSet("zookeeper", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&this.flw, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).require("-c").invalid(args) {
		return 2
	}

	validCmds := []string{
		"conf",
		"cons",
		"dump",
		"envi",
		"reqs",
		"ruok",
		"srvr",
		"stat",
		"wchs",
		"wchc",
		"wchp",
		"mntr",
	}
	foundCmd := false
	for _, c := range validCmds {
		this.cmdHelp += c + " "
		if this.flw == c {
			foundCmd = true
			break
		}
	}

	if !foundCmd {
		this.Ui.Error(fmt.Sprintf("invalid command: %s", this.flw))
		this.Ui.Info(this.Help())
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
	this.Ui.Output(color.Blue(zkzone.Name()))
	for zkhost, lines := range zkzone.RunZkFourLetterCommand(this.flw) {
		this.Ui.Output(fmt.Sprintf("%s\n%s", color.Green("%28s", zkhost), lines))
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

    -c zk four letter word command
      conf cons dump envi reqs ruok srvr stat wchs wchc wchp mntr
`, this.Cmd)
	return strings.TrimSpace(help)
}
