package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Allocation struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Allocation) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
	)
	cmdFlags := flag.NewFlagSet("allocation", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.EsDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	handleCatCommand(this.Ui, zkzone, cluster, "allocation", "h=shards,disk.total,disk.used,ip,node")
	return
}

func (*Allocation) Synopsis() string {
	return "Display #shards and disk space used by data node"
}

func (this *Allocation) Help() string {
	help := fmt.Sprintf(`
Usage: %s allocation [options]

    %s

Options:

    -z zone

    -c cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
