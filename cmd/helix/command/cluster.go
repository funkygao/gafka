package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Cluster struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Cluster) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("cluster", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (*Cluster) Synopsis() string {
	return "Cluster management"
}

func (this *Cluster) Help() string {
	help := fmt.Sprintf(`
Usage: %s cluster [options]

    %s

Options:

    -z zone

    -add cluster

    -drop cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
