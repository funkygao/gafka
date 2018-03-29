package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Host struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Host) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("host", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (*Host) Synopsis() string {
	return "List servers of an application in production environment"
}

func (this *Host) Help() string {
	help := fmt.Sprintf(`
Usage: %s host

    %s

Options:

    -app appName

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
