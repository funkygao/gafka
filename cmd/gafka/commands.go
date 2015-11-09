package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/gafka/command"
	"github.com/mitchellh/cli"
)

var commands map[string]cli.CommandFactory

func init() {
	ui := &cli.BasicUi{Writer: os.Stdout}

	commands = map[string]cli.CommandFactory{
		"brokers": func() (cli.Command, error) {
			return &command.Brokers{
				Ui: ui,
			}, nil
		},

		"topics": func() (cli.Command, error) {
			return &command.Topics{
				Ui: ui,
			}, nil
		},
	}

}
