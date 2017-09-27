package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Shards struct {
	Ui  cli.Ui
	Cmd string

	index string
}

func (this *Shards) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
	)
	cmdFlags := flag.NewFlagSet("shards", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.EsDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&this.index, "i", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	handleCatCommand(this.Ui, zkzone, cluster, "shards")
	return
}

func (*Shards) Synopsis() string {
	return "Detailed view of what nodes contain which shards"
}

func (this *Shards) Help() string {
	help := fmt.Sprintf(`
Usage: %s shards [options]

    %s

Options:

    -z zone

    -c cluster

    -i index

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
