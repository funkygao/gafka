package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/pubsub/command"
	"github.com/funkygao/gocli"
)

var commands map[string]cli.CommandFactory

func init() {
	ui := &cli.BasicUi{Writer: os.Stdout}

	commands = map[string]cli.CommandFactory{
		"pub": func() (cli.Command, error) {
			return &command.Pub{
				Ui: ui,
			}, nil
		},

		"sub": func() (cli.Command, error) {
			return &command.Sub{
				Ui: ui,
			}, nil
		},

		"inbox": func() (cli.Command, error) {
			return &command.Inbox{
				Ui: ui,
			}, nil
		},

		"outbox": func() (cli.Command, error) {
			return &command.Outbox{
				Ui: ui,
			}, nil
		},

		"bind": func() (cli.Command, error) {
			return &command.Bind{
				Ui: ui,
			}, nil
		},

		"app": func() (cli.Command, error) {
			return &command.App{
				Ui: ui,
			}, nil
		},
	}

}
