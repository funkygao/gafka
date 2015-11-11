package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/config"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Controllers struct {
	Ui cli.Ui
}

func (this *Controllers) Run(args []string) (exitCode int) {
	var (
		cluster string
		zone    string
	)
	cmdFlags := flag.NewFlagSet("controllers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "") // TODO not used
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" {
		forAllZones(func(zone string, zkzone *zk.ZkZone) {
			this.printControllers(zone, zkzone)
		})

		return
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(config.ZonePath(zone)))
	this.printControllers(zone, zkzone)

	return
}

// Print all controllers of all clusters within a zone.
func (this *Controllers) printControllers(zone string, zkzone *zk.ZkZone) {
	this.Ui.Output(zone)
	zkzone.WithinControllers(func(cluster string, controller *zk.Controller) {
		this.Ui.Output(strings.Repeat(" ", 4) + cluster)
		if controller == nil {
			this.Ui.Output(fmt.Sprintf("\t%s", color.Red("empty")))
		} else {
			this.Ui.Output(fmt.Sprintf("\t%s", controller))
		}
	})

}

func (*Controllers) Synopsis() string {
	return "Print available kafka controllers from Zookeeper"
}

func (*Controllers) Help() string {
	help := `
Usage: gafka controllers [options]

	Print available kafka controllers from Zookeeper

Options:

  -z zone
  	Only print kafka controllers within this zone.

  -c cluster

`
	return strings.TrimSpace(help)
}
