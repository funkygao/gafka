package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Zones struct {
	Ui cli.Ui
}

func (this *Zones) Run(args []string) (exitCode int) {
	if len(args) > 0 {
		// user specified the zones to print
		for _, name := range args {
			if zk, present := cf.Zones[name]; present {
				this.Ui.Output(fmt.Sprintf("%8s: %s", name, zk))
			} else {
				this.Ui.Output(fmt.Sprintf("%8s: not defined", name))
			}
		}

		return
	}

	// print all by default
	for name, zkAddrs := range cf.Zones {
		this.Ui.Output(fmt.Sprintf("%8s: %s", name, zkAddrs))
	}

	return

}

func (*Zones) Synopsis() string {
	return "Print available zones defined in /etc/gafka.cf"
}

func (*Zones) Help() string {
	help := `
Usage: gafka zones [zone ...]
`
	return strings.TrimSpace(help)
}
