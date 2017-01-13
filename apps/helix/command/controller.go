package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/go-helix"
	"github.com/funkygao/gocli"
)

type Controller struct {
	Ui  cli.Ui
	Cmd string

	admin   helix.HelixAdmin
	cluster string
}

func (this *Controller) Run(args []string) (exitCode int) {
	var zone string
	cmdFlags := flag.NewFlagSet("controller", flag.ContinueOnError)
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

	this.Ui.Info(fmt.Sprintf("leader: %s", this.admin.ControllerLeader(this.cluster)))
	history, err := this.admin.ControllerHistory(this.cluster)
	must(err)
	this.Ui.Output("history:")
	for _, hostPort := range history {
		this.Ui.Output(hostPort)
	}

	return
}

func (*Controller) Synopsis() string {
	return "Controller of a cluster"
}

func (this *Controller) Help() string {
	help := fmt.Sprintf(`
Usage: %s node [options]

    %s

Options:

    -z zone

    -c cluster

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
