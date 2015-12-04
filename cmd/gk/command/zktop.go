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

	if zone == "" {
		forSortedZones(func(zkzone *zk.ZkZone) {
			this.displayZoneTop(zkzone)
		})
	} else {
		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		this.displayZoneTop(zkzone)
	}

	return
}

func (this *Zktop) displayZoneTop(zkzone *zk.ZkZone) {
	this.Ui.Output(color.Green(zkzone.Name()))

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
