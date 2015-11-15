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

type Controllers struct {
	Ui  cli.Ui
	Cmd string
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

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZonePath(zone)))
	this.printControllers(zone, zkzone)

	return
}

// Print all controllers of all clusters within a zone.
func (this *Controllers) printControllers(zone string, zkzone *zk.ZkZone) {
	this.Ui.Output(zone)
	n := 0
	zkzone.WithinControllers(func(cluster string, controller *zk.Controller) {
		this.Ui.Output(strings.Repeat(" ", 4) + cluster)
		if controller == nil {
			this.Ui.Output(fmt.Sprintf("\t%s", color.Red("empty")))
		} else {
			n++

			this.Ui.Output(fmt.Sprintf("\t%s", controller))
		}
	})
	this.Ui.Output(fmt.Sprintf("%80d", n))

}

func (*Controllers) Synopsis() string {
	return "Print active controllers in kafka clusters"
}

func (this *Controllers) Help() string {
	help := fmt.Sprintf(`
Usage: %s controllers [options]

	Print active controllers in kafka clusters

Options:

  -z zone
  	Only print kafka controllers within this zone.

  -c cluster

`, this.Cmd)
	return strings.TrimSpace(help)
}
