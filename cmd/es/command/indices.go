package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Indices struct {
	Ui  cli.Ui
	Cmd string

	indexPattern string
}

func (this *Indices) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
	)
	cmdFlags := flag.NewFlagSet("indices", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.EsDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&this.indexPattern, "i", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	handleCatCommand(this.Ui, zkzone, cluster, "indices")
	return
}

func (*Indices) Synopsis() string {
	return "List indices"
}

func (this *Indices) Help() string {
	help := fmt.Sprintf(`
Usage: %s indices [options]

    %s

Options:

    -z zone

    -c cluster

    -i index pattern

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
