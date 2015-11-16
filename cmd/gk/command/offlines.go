package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Offlines struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Offlines) Run(args []string) (exitCode int) {
	if validateArgs(this, this.Ui).require("-z", "-c").invalid(args) {
		return 2
	}

	cmd := Brokers{
		Ui:  this.Ui,
		Cmd: this.Cmd,
	}
	args = append(args, "-stale")
	return cmd.Run(args)
}

func (*Offlines) Synopsis() string {
	return "Display all offline brokers"
}

func (this *Offlines) Help() string {
	help := fmt.Sprintf(`
Usage: %s offlines [options]

	Display all offline brokers

Options:

  -z zone
  	Only print brokers within a zone.

  -c cluster name
  	Only print brokers of this cluster.

`, this.Cmd)
	return strings.TrimSpace(help)
}
