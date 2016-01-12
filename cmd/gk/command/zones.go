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
	// header
	this.Ui.Output(fmt.Sprintf("%8s %-70s", "zone", "zookeeper ensemble"))
	this.Ui.Output(fmt.Sprintf("%s %s",
		strings.Repeat("-", 8),
		strings.Repeat("-", 70)))

	if len(args) > 0 {
		// user specified the zones to print
		for _, zone := range args {
			if zk, present := ctx.Zones()[zone]; present {
				this.Ui.Output(fmt.Sprintf("%8s %s", zone, zk))
			} else {
				this.Ui.Output(fmt.Sprintf("%8s not defined", zone))
			}
		}

		return
	}

	// print all by default
	for _, zone := range ctx.SortedZones() {
		this.Ui.Output(fmt.Sprintf("%8s %s", zone, ctx.NamedZoneZkAddrs(zone)))
	}

	return

}

func (*Zones) Synopsis() string {
	return "Print zones defined in $HOME/.gafka.cf"
}

func (this *Zones) Help() string {
	help := fmt.Sprintf(`
Usage: %s zones [zone ...]

    Print zones defined in $HOME/.gafka.cf
`, this.Cmd)
	return strings.TrimSpace(help)
}
