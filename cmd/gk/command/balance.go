package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Balance struct {
	Ui  cli.Ui
	Cmd string

	zone    string
	cluster string
}

func (this *Balance) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("balance", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (*Balance) Synopsis() string {
	return "Balance topics distribution according to load instead of count"
}

func (this *Balance) Help() string {
	help := fmt.Sprintf(`
Usage: %s balance [options]

    %s

Options:

    -z zone

    -c cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
