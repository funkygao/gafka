package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/funkygao/gafka/cmd/gk/command"
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
		"zones": func() (cli.Command, error) {
			return &command.Zones{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"alias": func() (cli.Command, error) {
			return &command.Alias{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"?": func() (cli.Command, error) {
			return &command.Faq{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"ping": func() (cli.Command, error) {
			return &command.Ping{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"config": func() (cli.Command, error) {
			return &command.Config{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"lszk": func() (cli.Command, error) {
			return &command.LsZk{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"mount": func() (cli.Command, error) {
			return &command.Mount{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"offset": func() (cli.Command, error) {
			return &command.Offset{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"migrate": func() (cli.Command, error) {
			return &command.Migrate{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"zktop": func() (cli.Command, error) {
			return &command.Zktop{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"kateway": func() (cli.Command, error) {
			return &command.Kateway{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"deploy": func() (cli.Command, error) {
			return &command.Deploy{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"discover": func() (cli.Command, error) {
			return &command.Discover{
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

		"console": func() (cli.Command, error) {
			return &command.Console{
				Ui:   ui,
				Cmd:  cmd,
				Cmds: commands,
			}, nil
		},

		/* TODOs
		"consul": func() (cli.Command, error) {
			return &command.Consul{
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

		"producers": func() (cli.Command, error) {
			return &command.Producers{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"ssh": func() (cli.Command, error) {
			return &command.Ssh{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"verifyreplicas": func() (cli.Command, error) {
			return &command.VerifyReplicas{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"audit": func() (cli.Command, error) {
			return &command.Audit{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		}, */

		"zookeeper": func() (cli.Command, error) {
			return &command.Zookeeper{
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

func makeShutdownCh() <-chan struct{} {
	ch := make(chan struct{})

	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		for {
			<-sigCh
			ch <- struct{}{}
		}
	}()
	return ch
}
