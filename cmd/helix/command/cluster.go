package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
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

	this.admin = getConnectedAdmin(zone)
	defer this.admin.Disconnect()

	switch {
	case addCluster != "":
		must(this.admin.AddCluster(addCluster))
		this.Ui.Info(fmt.Sprintf("%s added", addCluster))

	case dropCluster != "":
		must(this.admin.DropCluster(dropCluster))
		this.Ui.Info(fmt.Sprintf("%s dropped", dropCluster))

	default:
		clusters, err := this.admin.Clusters()
		must(err)
		for _, c := range clusters {
			this.Ui.Output(c)
		}
	}

	return
}

func (*Cluster) Synopsis() string {
	return "Cluster management within a zone"
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
