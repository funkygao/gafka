package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/kconsole/command"
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
		"zones": func() (cli.Command, error) {
			return &command.Zones{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"?": func() (cli.Command, error) {
			return &command.Faq{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"sample": func() (cli.Command, error) {
			return &command.Sample{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"lag": func() (cli.Command, error) {
			return &command.Lag{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"clusters": func() (cli.Command, error) {
			return &command.Clusters{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"peek": func() (cli.Command, error) {
			return &command.Peek{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
