package main

import (
	"os"

	"github.com/funkygao/gafka/misc/canal/command"
	"github.com/funkygao/gocli"
)

var commands map[string]cli.CommandFactory

func init() {
	ui := &cli.BasicUi{
		Writer:      os.Stdout,
		Reader:      os.Stdin,
		ErrorWriter: os.Stderr,
	}
	cmd := os.Args[0]

	commands = map[string]cli.CommandFactory{
		"binlog": func() (cli.Command, error) {
			return &command.Binlog{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"canal": func() (cli.Command, error) {
			return &command.Canal{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
