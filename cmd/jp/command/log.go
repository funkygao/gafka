package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Log struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Log) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("log", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing key")
		return 2
	}

	return
}

func (*Log) Synopsis() string {
	return "Tail online logs of an application"
}

func (this *Log) Help() string {
	help := fmt.Sprintf(`
Usage: %s log key

    %s

Options:

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
