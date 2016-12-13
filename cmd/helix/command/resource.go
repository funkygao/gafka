package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/gocli"
)

type Resource struct {
	Ui  cli.Ui
	Cmd string

	admin   helix.HelixAdmin
	cluster string
}

func (this *Resource) Run(args []string) (exitCode int) {
	var zone string
	cmdFlags := flag.NewFlagSet("resource", flag.ContinueOnError)
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if this.cluster == "" {
		this.Ui.Error("-c required")
		return 2
	}

	this.admin = getConnectedAdmin(zone)
	defer this.admin.Disconnect()

	return
}

func (*Resource) Synopsis() string {
	return "Resources management of a cluster"
}

func (this *Resource) Help() string {
	help := fmt.Sprintf(`
Usage: %s resource [options]

    %s

Options:

    -z zone

    -c cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
