package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Agent struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Agent) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("agent", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (*Agent) Synopsis() string {
	return "Starts the gk agent and runs until an interrupt is received"
}

func (this *Agent) Help() string {
	help := fmt.Sprintf(`
Usage: %s agent [options]

    Starts the gk agent and runs until an interrupt is received. 
    The agent represents a single node in a zone.

`, this.Cmd)
	return strings.TrimSpace(help)
}
