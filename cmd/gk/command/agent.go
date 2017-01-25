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
		install     bool
		listMembers bool
		start       bool
		port        int
		seeds       string
	)
	cmdFlags := flag.NewFlagSet("agent", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&install, "install", false, "")
	cmdFlags.BoolVar(&start, "start", false, "")
	cmdFlags.IntVar(&port, "port", 10114, "")
	cmdFlags.BoolVar(&listMembers, "l", false, "")
	cmdFlags.StringVar(&seeds, "join", "", "")
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
		seedNodes := make([]string, 0)
		for _, node := range strings.Split(seeds, ",") {
			node = strings.TrimSpace(node)
			if node == "" {
				continue
			}

			seedNodes = append(seedNodes, node)
		}
		agent.New().ServeForever(port, seedNodes...)
	}

	if listMembers {
		agent.New().ListMembers()
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

    -port port
      Defaults 10114

    -join seeds
      Comma seperated host:port

    -l
      List members

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
