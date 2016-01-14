package command

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Zookeeper struct {
	Ui  cli.Ui
	Cmd string

	flw        string // zk four letter word command
	cmdHelp    string
	zkHost     string
	watchMode  bool
	leaderOnly bool
}

func (this *Zookeeper) Run(args []string) (exitCode int) {
	var (
		zone string
	)
	cmdFlags := flag.NewFlagSet("zookeeper", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&this.flw, "c", "", "")
	cmdFlags.StringVar(&this.zkHost, "host", "", "")
	cmdFlags.BoolVar(&this.watchMode, "watch", false, "")
	cmdFlags.BoolVar(&this.leaderOnly, "leader", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if !this.leaderOnly && validateArgs(this, this.Ui).require("-c").invalid(args) {
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

	if !foundCmd && !this.leaderOnly {
		this.Ui.Error(fmt.Sprintf("invalid command: %s", this.flw))
		this.Ui.Info(this.Help())
		return 2
	}

	if this.watchMode {
		refreshScreen()
	}

	if zone != "" {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		defer printSwallowedErrors(this.Ui, zkzone)

		if this.leaderOnly {
			this.printLeader(zkzone)
		} else {
			this.printZkStats(zkzone)
		}

		return
	}

	// print all zones by default
	forSortedZones(func(zkzone *zk.ZkZone) {
		if this.leaderOnly {
			this.printLeader(zkzone)
		} else {
			this.printZkStats(zkzone)
		}

		printSwallowedErrors(this.Ui, zkzone)
	})

	return
}

func (this *Zookeeper) printLeader(zkzone *zk.ZkZone) {
	// FIXME all zones will only show the 1st zone info because it blocks others
	for {
		this.Ui.Output(color.Blue(zkzone.Name()))
		for zkhost, lines := range zkzone.RunZkFourLetterCommand("mntr") {
			if this.zkHost != "" && !strings.HasPrefix(zkhost, this.zkHost+":") {
				continue
			}

			parts := strings.Split(lines, "\n")
			for _, l := range parts {
				if strings.HasPrefix(l, "zk_server_state") && strings.HasSuffix(l, "leader") {
					this.Ui.Output(color.Green("%28s", zkhost))
					break
				}
			}
		}

		if this.watchMode {
			time.Sleep(time.Second * 5)
		} else {
			break
		}
	}

}

func (this *Zookeeper) printZkStats(zkzone *zk.ZkZone) {
	for {
		this.Ui.Output(color.Blue(zkzone.Name()))
		for zkhost, lines := range zkzone.RunZkFourLetterCommand(this.flw) {
			if this.zkHost != "" && !strings.HasPrefix(zkhost, this.zkHost+":") {
				continue
			}

			this.Ui.Output(fmt.Sprintf("%s\n%s", color.Green("%28s", zkhost), lines))
		}

		if this.watchMode {
			time.Sleep(time.Second * 5)
		} else {
			break
		}
	}

}

func (*Zookeeper) Synopsis() string {
	return "Display zone Zookeeper status by four letter word command"
}

func (this *Zookeeper) Help() string {
	help := fmt.Sprintf(`
Usage: %s zookeeper [options]

    Display zone Zookeeper status by four letter word command

Options:

    -z zone

    -host ip
      Display the zk host only.

    -watch
      Watch mode.

    -leader
      Display zk leader only.

    -c zk four letter word command
      conf cons dump envi reqs ruok srvr stat wchs wchc wchp mntr
`, this.Cmd)
	return strings.TrimSpace(help)
}
