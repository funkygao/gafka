package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/gzk/command"
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
		"ls": func() (cli.Command, error) {
			return &command.Ls{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"stat": func() (cli.Command, error) {
			return &command.Stat{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"zones": func() (cli.Command, error) {
			return &command.Zones{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"get": func() (cli.Command, error) {
			return &command.Get{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
