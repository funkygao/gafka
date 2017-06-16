package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Nodes struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Nodes) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
		attrs   bool
	)
	cmdFlags := flag.NewFlagSet("nodes", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&attrs, "attrs", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	if attrs {
		handleCatCommand(this.Ui, zkzone, cluster, "nodeattrs")
		return
	}

	handleCatCommand(this.Ui, zkzone, cluster, "nodes")

	return
}

func (*Nodes) Synopsis() string {
	return "Display nodes of cluster"
}

func (this *Nodes) Help() string {
	help := fmt.Sprintf(`
Usage: %s nodes [options]

    %s

Options:

    -z zone

    -c cluster

    -attrs
     Display node attributes.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
