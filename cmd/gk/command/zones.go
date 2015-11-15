package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Zones struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Zones) Run(args []string) (exitCode int) {
	if len(args) > 0 {
		// user specified the zones to print
		for _, name := range args {
			if zk, present := ctx.Zones()[name]; present {
				this.Ui.Output(fmt.Sprintf("%8s: %s", name, zk))
			} else {
				this.Ui.Output(fmt.Sprintf("%8s: not defined", name))
			}
		}

		return
	}

	// print all by default
	for _, zone := range ctx.SortedZones() {
		this.Ui.Output(fmt.Sprintf("%8s: %s", zone, ctx.ZonePath(zone)))
	}

	return

}

func (*Zones) Synopsis() string {
	return "Print zones defined in /etc/gafka.cf"
}

func (this *Zones) Help() string {
	help := fmt.Sprintf(`
Usage: %s zones [zone ...]

	Print zones defined in /etc/gafka.cf
`, this.Cmd)
	return strings.TrimSpace(help)
}
