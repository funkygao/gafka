package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/columnize"
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
		cluster    string
		addCluster string
		bootNode   string
	)
	cmdFlags := flag.NewFlagSet("clusters", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.EsDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&bootNode, "bootnode", "", "")
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

	case bootNode != "":
		if cluster == "" {
			this.Ui.Error("-c required")
			return 2
		}

		ec := zkzone.NewEsCluster(cluster)
		ec.AddNode(bootNode)

	default:
		lines := []string{"Custer|Bootstrap"}
		zkzone.ForSortedEsClusters(func(ec *zk.EsCluster) {
			lines = append(lines, fmt.Sprintf("%s|%s", ec.Name, strings.Join(ec.Nodes(), ",")))
		})
		this.Ui.Output(columnize.SimpleFormat(lines))
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

    -c cluster

    -add name
     Add a new ElasticSearch cluster.

    -bootnode host:port
     Add a bootstrap node to a cluster.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
