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
		zone string
		add  string
		drop string
	)
	cmdFlags := flag.NewFlagSet("cluster", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&add, "add", "", "")
	cmdFlags.StringVar(&drop, "drop", "", "")

	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.admin = getConnectedAdmin(zone)
	defer this.admin.Disconnect()

	switch {
	case add != "":
		must(this.admin.AddCluster(add))
		this.Ui.Info(fmt.Sprintf("%s added", add))

	case drop != "":
		must(this.admin.DropCluster(drop))
		this.Ui.Info(fmt.Sprintf("%s dropped", drop))

	default:
		clusters, err := this.admin.Clusters()
		must(err)
		for _, c := range clusters {
			this.Ui.Output(c)

			// TODO get cluster config
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
