package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Top struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Top) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
	)
	cmdFlags := flag.NewFlagSet("top", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}
	return
}

func (*Top) Synopsis() string {
	return "Unix “top” like utility for ElasticSearch"
}

func (this *Top) Help() string {
	help := fmt.Sprintf(`
Usage: %s top

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
