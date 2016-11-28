package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Resource struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Resource) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("resource", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (*Resource) Synopsis() string {
	return "Resource management"
}

func (this *Resource) Help() string {
	help := fmt.Sprintf(`
Usage: %s resource [options]

    %s

Options:

    -z zone

    -add cluster

    -drop cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
