package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Rm struct {
	Ui  cli.Ui
	Cmd string

	zone      string
	path      string
	recursive bool
}

func (this *Rm) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("rm", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.path, "p", "", "")
	cmdFlags.BoolVar(&this.recursive, "R", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-p").
		invalid(args) {
		return 2
	}

	return
}

func (*Rm) Synopsis() string {
	return "Remove znode TODO"
}

func (this *Rm) Help() string {
	help := fmt.Sprintf(`
Usage: %s create -z zone -p path [options]

    Remove znode

`, this.Cmd)
	return strings.TrimSpace(help)
}
