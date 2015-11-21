package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/pretty"
)

type Zookeepers struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Zookeepers) Run(args []string) (exitCode int) {
	if len(args) > 0 {
		// user specified the zones to print
		for _, zone := range args {
			zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
			this.printZkStats(zkzone)
		}

		return
	}

	// print all by default
	forAllZones(func(zkzone *zk.ZkZone) {
		this.printZkStats(zkzone)
	})

	return

}

func (this *Zookeepers) printZkStats(zkzone *zk.ZkZone) {
	this.Ui.Output(fmt.Sprintf("zone: %s", zkzone.Name()))
	this.Ui.Output(strings.Repeat("-", 100))
	for _, stat := range zkzone.ZkStats() {
		this.Ui.Output(fmt.Sprintf("%# v", pretty.Formatter(*stat)))
	}

}

func (*Zookeepers) Synopsis() string {
	return "Display zone Zookeeper status"
}

func (this *Zookeepers) Help() string {
	help := fmt.Sprintf(`
Usage: %s zookeepers [zone ...]

    Display zone Zookeeper status
`, this.Cmd)
	return strings.TrimSpace(help)
}
