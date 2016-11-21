package main

import (
	"os"

	"github.com/funkygao/gafka/apps/kafka/command"
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
		"partition": func() (cli.Command, error) {
			return &command.Partition{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"offset": func() (cli.Command, error) {
			return &command.OffsetManager{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"rebalance": func() (cli.Command, error) {
			return &command.Rebalance{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"doc": func() (cli.Command, error) {
			return &command.Doc{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"zksession": func() (cli.Command, error) {
			return &command.ZkSession{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"logdirs": func() (cli.Command, error) {
			return &command.LogDirs{
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

		"replica": func() (cli.Command, error) {
			return &command.Replica{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
