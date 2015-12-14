package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Shutdown struct {
	Ui   cli.Ui
	Cmd  string
	zone string
}

func (this *Shutdown) Run(args []string) (exitCode int) {
	if validateArgs(this, this.Ui).
		require("-z", "-ip").
		requireAdminRights("-z").
		invalid(args) {
		return 2
	}

	return
}

func (*Shutdown) Synopsis() string {
	return "Shutdown a broker by ip address TODO"
}

func (this *Shutdown) Help() string {
	help := fmt.Sprintf(`
Usage: %s shutdown -z zone -ip addr

    Shutdown a broker by ip

`, this.Cmd)
	return strings.TrimSpace(help)
}
