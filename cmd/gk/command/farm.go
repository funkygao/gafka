package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Farm struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Farm) Run(args []string) (exitCode int) {
	var (
		zone    string
		addNode string
	)
	cmdFlags := flag.NewFlagSet("farm", flag.ContinueOnError)
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&addNode, "add", "", "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	switch {
	case addNode != "":
		if err := zkzone.EnsurePathExists(fmt.Sprintf("/_farm/%s", addNode)); err != nil {
			this.Ui.Error(err.Error())
		} else {
			this.Ui.Infof("%s registered", addNode)
		}

	default:
		children, _, err := zkzone.Conn().Children("/_farm")
		swallow(err)
		for _, c := range children {
			this.Ui.Output(c)
		}
	}

	return
}

func (*Farm) Synopsis() string {
	return "Manage reserved servers farm to deploy new kafka brokers"
}

func (this *Farm) Help() string {
	help := fmt.Sprintf(`
Usage: %s farm [options]

    %s

Options:

    -add ip
     Add a new server to the server farm

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
