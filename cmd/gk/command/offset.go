package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Offset struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Offset) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
		topic   string
		group   string
	)
	cmdFlags := flag.NewFlagSet("offset", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&topic, "t", "", "")
	cmdFlags.StringVar(&group, "g", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-c", "-t", "-g").
		requireAdminRights("-z").
		invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	zkcluster := zkzone.NewCluster(cluster)
	zkcluster.ResetConsumerGroupOffset(topic, group)
	this.Ui.Output("done")
	return
}

func (*Offset) Synopsis() string {
	return "Manually reset consumer group offset"
}

func (this *Offset) Help() string {
	help := fmt.Sprintf(`
Usage: %s offset -z zone -c cluster -t topic -g group [options]

    Manually reset consumer group offset

Options:

`, this.Cmd)
	return strings.TrimSpace(help)
}
