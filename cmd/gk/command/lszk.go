package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type LsZk struct {
	Ui  cli.Ui
	Cmd string

	zone      string
	cluster   string
	recursive bool
}

func (this *LsZk) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("lszk", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.BoolVar(&this.recursive, "R", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	if this.cluster != "" {
		zkcluster := zkzone.NewCluster(this.cluster)
		this.printCluster(zkcluster)
	} else {
		zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
			this.printCluster(zkcluster)
		})
	}

	return
}

func (this *LsZk) printCluster(zkcluster *zk.ZkCluster) {
	this.Ui.Output(color.Green(zkcluster.Name()))
	children, err := zkcluster.ListChildren(this.recursive)
	if err != nil {
		this.Ui.Error(fmt.Sprintf("%s%s", strings.Repeat(" ", 4), err))
		return
	}

	for _, c := range children {
		this.Ui.Output(fmt.Sprintf("%s%s", strings.Repeat(" ", 4), c))
		if strings.HasSuffix(c, "brokers") {
			this.Ui.Output(fmt.Sprintf("%s%s/ids", strings.Repeat(" ", 4), c))
			this.Ui.Output(fmt.Sprintf("%s%s/topics", strings.Repeat(" ", 4), c))
		}
	}
}

func (*LsZk) Synopsis() string {
	return "List kafka related zookeepeer znode children"
}

func (this *LsZk) Help() string {
	help := fmt.Sprintf(`
Usage: %s lszk [options] <path>

    %s

Options:

    -z zone
      Default %s

    -c cluster

    -R
      recursive.    

`, this.Cmd, this.Synopsis(), ctx.ZkDefaultZone())
	return strings.TrimSpace(help)
}
