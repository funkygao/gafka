package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/helix/command"
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
		OutputColor: cli.UiColorNone,
		InfoColor:   cli.UiColorGreen,
		ErrorColor:  cli.UiColorRed,
		WarnColor:   cli.UiColorYellow,
	}
	cmd := os.Args[0]

	commands = map[string]cli.CommandFactory{
		"clusters": func() (cli.Command, error) {
			return &command.Cluster{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"nodes": func() (cli.Command, error) {
			return &command.Node{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"resources": func() (cli.Command, error) {
			return &command.Resource{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"controller": func() (cli.Command, error) {
			return &command.Controller{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"trace": func() (cli.Command, error) {
			return &command.Trace{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
