package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/es/command"
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
			return &command.Clusters{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"nodes": func() (cli.Command, error) {
			return &command.Nodes{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"merge": func() (cli.Command, error) {
			return &command.Merge{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"top": func() (cli.Command, error) {
			return &command.Top{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"indices": func() (cli.Command, error) {
			return &command.Indices{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"shards": func() (cli.Command, error) {
			return &command.Shards{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"checkup": func() (cli.Command, error) {
			return &command.Checkup{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"allocation": func() (cli.Command, error) {
			return &command.Allocation{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
