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

		"aliases": func() (cli.Command, error) {
			return &command.Aliases{
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

		"health": func() (cli.Command, error) {
			return &command.Health{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"count": func() (cli.Command, error) {
			return &command.Count{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"pending": func() (cli.Command, error) {
			return &command.Pending{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"plugins": func() (cli.Command, error) {
			return &command.Plugins{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"segments": func() (cli.Command, error) {
			return &command.Segments{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"threads": func() (cli.Command, error) {
			return &command.Threads{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
