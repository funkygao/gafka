package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/zk/command"
	"github.com/funkygao/gocli"
)

var commands map[string]cli.CommandFactory
var ui cli.Ui

func init() {
	ui = &cli.ColoredUi{
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

		"top": func() (cli.Command, error) {
			return &command.Zktop{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"acl": func() (cli.Command, error) {
			return &command.Acl{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"dump": func() (cli.Command, error) {
			return &command.Dump{
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

		"create": func() (cli.Command, error) {
			return &command.Create{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"rm": func() (cli.Command, error) {
			return &command.Rm{
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

		"set": func() (cli.Command, error) {
			return &command.Set{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"console": func() (cli.Command, error) {
			return &command.Console{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
