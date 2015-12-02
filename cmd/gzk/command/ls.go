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

type Ls struct {
	Ui  cli.Ui
	Cmd string

	zone      string
	cluster   string
	recursive bool
}

func (this *Ls) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("ls", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.BoolVar(&this.recursive, "R", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	if validateArgs(this, this.Ui).require("-z").invalid(args) {
		return 2
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(this.zone, ctx.ZoneZkAddrs(this.zone)))
	if this.cluster != "" {
		zkcluster := zkzone.NewCluster(this.cluster)
		this.printCluster(zkcluster)
	} else {
		zkzone.WithinClusters(func(zkcluster *zk.ZkCluster) {
			this.printCluster(zkcluster)
		})
	}

	return
}

func (this *Ls) printCluster(zkcluster *zk.ZkCluster) {
	this.Ui.Output(color.Green(zkcluster.Name()))
	children, err := zkcluster.ListChildren(zkcluster.Chroot(), this.recursive)
	if err != nil {
		this.Ui.Error(fmt.Sprintf("%s%s", strings.Repeat(" ", 4), err))
		return
	}

	for _, c := range children {
		this.Ui.Output(fmt.Sprintf("%s%s", strings.Repeat(" ", 4), c))
	}
}

func (*Ls) Synopsis() string {
	return "list znode children"
}

func (this *Ls) Help() string {
	help := fmt.Sprintf(`
Usage: %s ls -z zone [-R] [options] <path>

Options:

    -c cluster

    -R
      recursive.    

`, this.Cmd)
	return strings.TrimSpace(help)
}
