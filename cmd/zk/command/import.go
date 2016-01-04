package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Import struct {
	Ui  cli.Ui
	Cmd string

	zone string
	path string
}

func (this *Import) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("import", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.ZkDefaultZone(), "")
	cmdFlags.StringVar(&this.path, "p", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-p").
		requireAdminRights("-p").
		invalid(args) {
		return 2
	}

	if this.zone == "" {
		this.Ui.Error("unknown zone")
		return 2
	}

	return
}

func (*Import) Synopsis() string {
	return "Load directories and contents to Zookeeper TODO"
}

func (this *Import) Help() string {
	help := fmt.Sprintf(`
Usage: %s import [options]

    Load directories and contents to Zookeeper

Options:


`, this.Cmd)
	return strings.TrimSpace(help)
}
