package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Scale struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Scale) Run(args []string) (exitCode int) {
	var (
		partitions           int
		brokers              string
		zone, cluster, topic string
	)
	cmdFlags := flag.NewFlagSet("scale", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&topic, "t", "", "")
	cmdFlags.IntVar(&partitions, "partitions", 0, "")
	cmdFlags.StringVar(&brokers, "brokers", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		requireAdminRights("-z").
		require("-c", "-t", "-partitions", "-brokers").
		invalid(args) {
		return 2
	}

	if partitions < 1 {
		return 1
	}

	return
}

func (*Scale) Synopsis() string {
	return "Scale up a topic to specified brokers TODO"
}

func (this *Scale) Help() string {
	help := fmt.Sprintf(`
Usage: %s scale [options]

    %s

Options:

    -z zone

    -c cluster

    -t topic

    -partitions number
      How many partitions to add for this topic

    -brokers id1,id2,idN
      Where to place the new partitions

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
