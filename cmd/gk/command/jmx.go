package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Jmx struct {
	Ui  cli.Ui
	Cmd string

	zone    string
	cluster string
}

func (this *Jmx) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("jmx", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (*Jmx) Synopsis() string {
	return "Generate config for jmxtrans to monitor kafka with JMX"
}

func (this *Jmx) Help() string {
	help := fmt.Sprintf(`
Usage: %s jmx [options]

    %s

Options:

    -z zone

    -c cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
