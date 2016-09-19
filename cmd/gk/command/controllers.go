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
	"github.com/funkygao/golib/gofmt"
)

type Controllers struct {
	Ui  cli.Ui
	Cmd string

	cluster string
}

func (this *Controllers) Run(args []string) (exitCode int) {
	var (
		zone string
	)
	cmdFlags := flag.NewFlagSet("controllers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "") // TODO not used
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" {
		forSortedZones(func(zkzone *zk.ZkZone) {
			this.printControllers(zkzone)
			printSwallowedErrors(this.Ui, zkzone)
		})

		return
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	this.printControllers(zkzone)
	printSwallowedErrors(this.Ui, zkzone)

	return
}

// Print all controllers of all clusters within a zone.
func (this *Controllers) printControllers(zkzone *zk.ZkZone) {
	this.Ui.Output(zkzone.Name())
	zkzone.ForSortedControllers(func(cluster string, controller *zk.ControllerMeta) {
		if !patternMatched(cluster, this.cluster) {
			return
		}

		this.Ui.Output(strings.Repeat(" ", 4) + cluster)
		if controller == nil {
			this.Ui.Output(fmt.Sprintf("\t%s", color.Red("empty")))
		} else {
			epochSince := time.Since(controller.Mtime.Time())
			epochSinceStr := gofmt.PrettySince(controller.Mtime.Time())
			if epochSince < time.Hour*2*24 {
				epochSinceStr = color.Red(epochSinceStr)
			}
			this.Ui.Output(fmt.Sprintf("\t%-2s %21s epoch:%2s/%-20s uptime:%s",
				controller.Broker.Id, controller.Broker.Addr(),
				controller.Epoch,
				epochSinceStr,
				gofmt.PrettySince(controller.Broker.Uptime())))
		}
	})

}

func (*Controllers) Synopsis() string {
	return "Print active controllers in kafka clusters"
}

func (this *Controllers) Help() string {
	help := fmt.Sprintf(`
Usage: %s controllers [options]

    %s

Options:

    -z zone

    -c cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
