package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Clusters struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Clusters) Run(args []string) (exitCode int) {
	var (
		zone       string
		addCluster string
	)
	cmdFlags := flag.NewFlagSet("clusters", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&addCluster, "add", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	switch {
	case addCluster != "":
		if err := zkzone.CreateEsCluster(addCluster); err != nil {
			this.Ui.Error(err.Error())
		} else {
			this.Ui.Infof("%s created", addCluster)
		}

	default:
		zkzone.ForSortedEsClusters(func(ec *zk.EsCluster) {
			this.Ui.Output(ec.Name)
		})
	}

	return
}

func (*Clusters) Synopsis() string {
	return "ElasticSearch clusters"
}

func (this *Clusters) Help() string {
	help := fmt.Sprintf(`
Usage: %s clusters [options]

    %s

Options:

    -z zone

    -add name
     Add a new dbus cluster.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
