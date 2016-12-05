package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Node struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Node) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("node", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (*Node) Synopsis() string {
	return "Node management"
}

func (this *Node) Help() string {
	help := fmt.Sprintf(`
Usage: %s node [options]

    %s

Options:

    -z zone

    -add cluster

    -drop cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
