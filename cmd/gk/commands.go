package main

import (
	"os"

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
		OutputColor: cli.UiColorNone,
		InfoColor:   cli.UiColorGreen,
		ErrorColor:  cli.UiColorRed,
		WarnColor:   cli.UiColorYellow,
	}
	cmd := os.Args[0]

	commands = map[string]cli.CommandFactory{
		"zones": func() (cli.Command, error) {
			return &command.Zones{
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

		"haproxy": func() (cli.Command, error) {
			return &command.Haproxy{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"chaos": func() (cli.Command, error) {
			return &command.Chaos{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"time": func() (cli.Command, error) {
			return &command.Time{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"comma": func() (cli.Command, error) {
			return &command.Comma{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"cpu": func() (cli.Command, error) {
			return &command.Cpu{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"trace": func() (cli.Command, error) {
			return &command.Trace{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"gc": func() (cli.Command, error) {
			return &command.GC{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"capacity": func() (cli.Command, error) {
			return &command.Capacity{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"scale": func() (cli.Command, error) {
			return &command.Scale{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"merkle": func() (cli.Command, error) {
			return &command.Merkle{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"redis": func() (cli.Command, error) {
			return &command.Redis{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"lookup": func() (cli.Command, error) {
			return &command.Lookup{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"systool": func() (cli.Command, error) {
			return &command.Systool{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"influx": func() (cli.Command, error) {
			return &command.Influx{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"jmx": func() (cli.Command, error) {
			return &command.Jmx{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"balance": func() (cli.Command, error) {
			return &command.Balance{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"zklog": func() (cli.Command, error) {
			return &command.ZkLog{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"pps": func() (cli.Command, error) {
			return &command.Pps{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"histogram": func() (cli.Command, error) {
			return &command.Histogram{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"perf": func() (cli.Command, error) {
			return &command.Perf{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"sniff": func() (cli.Command, error) {
			return &command.Sniff{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"job": func() (cli.Command, error) {
			return &command.Job{
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

		"logstash": func() (cli.Command, error) {
			return &command.Logstash{
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

		"segment": func() (cli.Command, error) {
			return &command.Segment{
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

		"ext4": func() (cli.Command, error) {
			return &command.Ext4fs{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"agent": func() (cli.Command, error) {
			return &command.Agent{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"disable": func() (cli.Command, error) {
			return &command.Disable{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"produce": func() (cli.Command, error) {
			return &command.Produce{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"upgrade": func() (cli.Command, error) {
			return &command.Upgrade{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"move": func() (cli.Command, error) {
			return &command.Move{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"verify": func() (cli.Command, error) {
			return &command.Verify{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"whois": func() (cli.Command, error) {
			return &command.Whois{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"sample": func() (cli.Command, error) {
			return &command.Sample{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"zkinstall": func() (cli.Command, error) {
			return &command.ZkInstall{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"mirror": func() (cli.Command, error) {
			return &command.Mirror{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"autocomplete": func() (cli.Command, error) {
			return &command.Autocomplete{
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

		"webhook": func() (cli.Command, error) {
			return &command.Webhook{
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

		"kateway": func() (cli.Command, error) {
			return &command.Kateway{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		"kguard": func() (cli.Command, error) {
			return &command.Kguard{
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

		"topbroker": func() (cli.Command, error) {
			return &command.TopBroker{
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

		"members": func() (cli.Command, error) {
			return &command.Members{
				Ui:  ui,
				Cmd: cmd,
			}, nil
		},

		/* TODOs


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

		"zk": func() (cli.Command, error) {
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
