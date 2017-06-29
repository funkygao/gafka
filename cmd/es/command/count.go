package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Count struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Count) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
	)
	cmdFlags := flag.NewFlagSet("count", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.EsDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	handleCatCommand(this.Ui, zkzone, cluster, "count")

	return
}

func (*Count) Synopsis() string {
	return "Document count of the entire cluster"
}

func (this *Count) Help() string {
	help := fmt.Sprintf(`
Usage: %s count [options]

    %s

Options:

    -z zone

    -c cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
