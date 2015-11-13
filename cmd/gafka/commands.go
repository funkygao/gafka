package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/gafka/command"
	"github.com/funkygao/gocli"
)

var commands map[string]cli.CommandFactory

func init() {
	ui := &cli.BasicUi{Writer: os.Stdout}

	commands = map[string]cli.CommandFactory{
		"zones": func() (cli.Command, error) {
			return &command.Zones{
				Ui: ui,
			}, nil
		},

		"producers": func() (cli.Command, error) {
			return &command.Producers{
				Ui: ui,
			}, nil
		},

		"lags": func() (cli.Command, error) {
			return &command.Lags{
				Ui: ui,
			}, nil
		},

		"consumers": func() (cli.Command, error) {
			return &command.Consumers{
				Ui: ui,
			}, nil
		},

		"rebalance": func() (cli.Command, error) {
			return &command.Rebalance{
				Ui: ui,
			}, nil
		},

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

		"clusters": func() (cli.Command, error) {
			return &command.Clusters{
				Ui: ui,
			}, nil
		},

		"partition": func() (cli.Command, error) {
			return &command.Partition{
				Ui: ui,
			}, nil
		},

		"underreplicated": func() (cli.Command, error) {
			return &command.UnderReplicated{
				Ui: ui,
			}, nil
		},

		"audit": func() (cli.Command, error) {
			return &command.Audit{
				Ui: ui,
			}, nil
		},

		"controllers": func() (cli.Command, error) {
			return &command.Controllers{
				Ui: ui,
			}, nil
		},

		"peek": func() (cli.Command, error) {
			return &command.Peek{
				Ui: ui,
			}, nil
		},

		"top": func() (cli.Command, error) {
			return &command.Top{
				Ui: ui,
			}, nil
		},

		"topology": func() (cli.Command, error) {
			return &command.Topology{
				Ui: ui,
			}, nil
		},
	}

}
