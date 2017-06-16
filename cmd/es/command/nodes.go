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

type Nodes struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Nodes) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
		addNode string
	)
	cmdFlags := flag.NewFlagSet("nodes", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&addNode, "add", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	switch {
	case addNode != "":
		ec := zkzone.NewEsCluster(cluster)
		if err := ec.AddNode(addNode); err != nil {
			this.Ui.Error(err.Error())
		} else {
			this.Ui.Infof("%s added for cluster[%s]", addNode, cluster)
		}

	default: // list nodes
		if cluster != "" {
			ec := zkzone.NewEsCluster(cluster)
			for _, node := range ec.Nodes() {
				this.Ui.Output(node)
			}
			return
		}

		// zone wide
		lines := []string{"Cluster|Node"}
		zkzone.ForSortedEsClusters(func(ec *zk.EsCluster) {
			for _, node := range ec.Nodes() {
				lines = append(lines, fmt.Sprintf("%s|%s", ec.Name, node))
			}
		})
		if len(lines) > 1 {
			this.Ui.Output(columnize.SimpleFormat(lines))
		}
	}

	return
}

func (*Nodes) Synopsis() string {
	return "Nodes of cluster"
}

func (this *Nodes) Help() string {
	help := fmt.Sprintf(`
Usage: %s nodes [options]

    %s

Options:

    -z zone

    -c cluster

    -add host:port
     Add a new node.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
