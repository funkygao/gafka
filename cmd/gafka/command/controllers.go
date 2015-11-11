package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
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
		forAllZones(func(zone string, zkAddrs string, zkutil *zk.ZkUtil) {
			this.printControllers(zone, zkutil)
		})

		return
	}

	zkutil := zk.NewZkUtil(zk.DefaultConfig(cf.Zones[zone]))
	this.printControllers(zone, zkutil)

	return
}

// Print all controllers of all clusters within a zone.
func (this *Controllers) printControllers(zone string, zkutil *zk.ZkUtil) {
	this.Ui.Output(zone)
	for cluster, controller := range zkutil.GetControllers() {
		this.Ui.Output(strings.Repeat(" ", 4) + cluster)
		this.Ui.Output(fmt.Sprintf("\t%s", controller))
	}
}

func (*Controllers) Synopsis() string {
	return "Print available kafka clusters from Zookeeper"
}

func (*Controllers) Help() string {
	help := `
Usage: gafka controllers [options]

	Print available kafka clusters from Zookeeper

Options:

  -z zone
  	Only print kafka controllers within this zone.

  -c cluster

`
	return strings.TrimSpace(help)
}
