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

	admin helix.HelixAdmin
}

func (this *Resource) Run(args []string) (exitCode int) {
	var zone string
	cmdFlags := flag.NewFlagSet("resource", flag.ContinueOnError)
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.admin = getConnectedAdmin(zone)
	defer this.admin.Disconnect()

	return
}

func (*Resource) Synopsis() string {
	return "Resource management"
}

func (this *Resource) Help() string {
	help := fmt.Sprintf(`
Usage: %s resource [options]

    %s

Options:

    -z zone

   

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
