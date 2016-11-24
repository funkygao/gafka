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

	// netstat -s | grep 'listen queue of a socket overflowed'
	// ss -no state established | tail -n +2 | awk '{print $1,$2}'

	return
}

func (*Agent) Synopsis() string {
	return "Starts the gk agent daemon to collect local status"
}

func (this *Agent) Help() string {
	help := fmt.Sprintf(`
Usage: %s agent [options]

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
