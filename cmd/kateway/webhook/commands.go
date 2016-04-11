package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/webhook/command/agent"
	"github.com/funkygao/gocli"
)

var commands map[string]cli.CommandFactory

func init() {
	ui := &cli.ColoredUi{
		Ui: &cli.BasicUi{
			Writer:      os.Stdout,
			Reader:      os.Stdin,
			ErrorWriter: os.Stderr,
		},
		InfoColor:  cli.UiColorGreen,
		ErrorColor: cli.UiColorRed,
		WarnColor:  cli.UiColorYellow,
	}
	cmd := os.Args[0]

	commands = map[string]cli.CommandFactory{
		"agent": func() (cli.Command, error) {
			return &agent.Agent{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
