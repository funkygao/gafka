package main

import (
	"os"

	"github.com/funkygao/gafka/cmd/gk/command"
	"github.com/funkygao/gocli"
)

var commands map[string]cli.CommandFactory

func init() {
	ui := &cli.BasicUi{Writer: os.Stdout}
	cmd := os.Args[0]

	commands = map[string]cli.CommandFactory{
		"zones": func() (cli.Command, error) {
			return &command.Zones{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"console": func() (cli.Command, error) {
			return &command.Console{
				Ui:   ui,
				Cmd:  cmd,
				Cmds: commands,
			}, nil
		},

		"stalebrokers": func() (cli.Command, error) {
			return &command.StaleBrokers{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"producers": func() (cli.Command, error) {
			return &command.Producers{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"lags": func() (cli.Command, error) {
			return &command.Lags{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"consumers": func() (cli.Command, error) {
			return &command.Consumers{
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

		"brokers": func() (cli.Command, error) {
			return &command.Brokers{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"topics": func() (cli.Command, error) {
			return &command.Topics{
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

		"partition": func() (cli.Command, error) {
			return &command.Partition{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"underreplicated": func() (cli.Command, error) {
			return &command.UnderReplicated{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"audit": func() (cli.Command, error) {
			return &command.Audit{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"controllers": func() (cli.Command, error) {
			return &command.Controllers{
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

		"top": func() (cli.Command, error) {
			return &command.Top{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"topology": func() (cli.Command, error) {
			return &command.Topology{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},
	}

}
