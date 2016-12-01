package command

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/go-helix/store/zk"
	"github.com/funkygao/gocli"
)

type Cluster struct {
	Ui  cli.Ui
	Cmd string

	admin helix.HelixAdmin
}

func (this *Cluster) Run(args []string) (exitCode int) {
	var (
		zone        string
		addCluster  string
		dropCluster string
	)
	cmdFlags := flag.NewFlagSet("cluster", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&addCluster, "add", "", "")
	cmdFlags.StringVar(&dropCluster, "drop", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.admin = zk.NewZKHelixAdmin(ctx.Zone(zone).ZkHelix, zk.WithSessionTimeout(time.Second*30))
	if err := this.admin.Connect(); err != nil {
		this.Ui.Error(err.Error())
		return 2
	}

	switch {
	case addCluster != "":
		err := this.admin.AddCluster(addCluster)
		if err != nil {
			this.Ui.Error(err.Error())
		} else {
			this.Ui.Info(fmt.Sprintf("%s added", addCluster))
		}

	case dropCluster != "":
		err := this.admin.DropCluster(dropCluster)
		if err != nil {
			this.Ui.Error(err.Error())
		} else {
			this.Ui.Info(fmt.Sprintf("%s dropped", dropCluster))
		}

	default:
		clusters, err := this.admin.Clusters()
		if err != nil {
			this.Ui.Error(err.Error())
		} else {
			for _, c := range clusters {
				this.Ui.Output(c)
			}
		}
	}

	return
}

func (*Cluster) Synopsis() string {
	return "Cluster management"
}

func (this *Cluster) Help() string {
	help := fmt.Sprintf(`
Usage: %s cluster [options]

    %s

Options:

    -z zone

    -add cluster

    -drop cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
