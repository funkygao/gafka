package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Prodtest struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Prodtest) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("prodtest", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (this *Prodtest) Synopsis() string {
	return "Run unit tests to find misconfigurations"
}

func (this *Prodtest) Help() string {
	help := fmt.Sprintf(`
Usage: %s prodtest [options]

    %s

Options:


`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
