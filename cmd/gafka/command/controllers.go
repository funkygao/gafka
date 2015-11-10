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
	cmdFlags.StringVar(&cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" {
		forAllZones(func(zone string, zkAddrs string, zkutil *zk.ZkUtil) {
			this.Ui.Output(zone)
			for cluster, controller := range zkutil.GetControllers() {
				this.Ui.Output(strings.Repeat(" ", 4) + cluster)
				this.Ui.Output(fmt.Sprintf("\t%s", controller))
			}
		})
	}

	return
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

  -c cluster name

`
	return strings.TrimSpace(help)
}
