package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/ryanuber/columnize"
)

type Clusters struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Clusters) Run(args []string) (exitCode int) {
	var zone string
	cmdFlags := flag.NewFlagSet("clusters", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.ZkDefaultZone(), "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	this.printClusters(zkzone)

	return
}

func (this *Clusters) printClusters(zkzone *zk.ZkZone) {
	lines := make([]string, 0)
	header := "Cluster|ZkConnect"
	lines = append(lines, header)
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		lines = append(lines,
			fmt.Sprintf("%s|%s", zkcluster.Name(), zkcluster.NamedZkConnectAddr()))
	})

	this.Ui.Output(columnize.SimpleFormat(lines))
}

func (*Clusters) Synopsis() string {
	return "Display kafka clusters in a zone"
}

func (this *Clusters) Help() string {
	help := fmt.Sprintf(`
Usage: %s clusters -z zone

    Display kafka clusters in a zone

`, this.Cmd)
	return strings.TrimSpace(help)
}
