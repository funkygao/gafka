package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Dump struct {
	Ui  cli.Ui
	Cmd string

	zone string
	path string
}

func (this *Dump) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("dump", flag.ContinueOnError)
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

func (*Dump) Synopsis() string {
	return "Dump directories and contents of Zookeeper TODO"
}

func (this *Dump) Help() string {
	help := fmt.Sprintf(`
Usage: %s dump [options]

    Dump directories and contents of Zookeeper

Options:

`, this.Cmd)
	return strings.TrimSpace(help)
}
