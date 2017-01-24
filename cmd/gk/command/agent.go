package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/cmd/gk/command/agent"
	"github.com/funkygao/gocli"
)

type Agent struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Agent) Run(args []string) (exitCode int) {
	var (
		install bool
		start   bool
	)
	cmdFlags := flag.NewFlagSet("agent", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&install, "install", false, "")
	cmdFlags.BoolVar(&start, "start", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if install {
		writeFileFromTemplate("template/init.d/gkagent", "/etc/init.d/gkagent", 0644, nil, nil)
		runCmd("chkconfig", []string{"--add", "gkagent"})
		this.Ui.Info("installed")
		return
	}

	if start {
		a := agent.New()
		a.Start()
	}

	return
}

func (*Agent) Synopsis() string {
	return "Starts the gk agent daemon which support multiple DC"
}

func (this *Agent) Help() string {
	help := fmt.Sprintf(`
Usage: %s agent [options]

    %s

Options:

    -install
      Install init script for gk agent

    -start
      Start gk agent daemon

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
