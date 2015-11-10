package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Zones struct {
	Ui cli.Ui
}

func (t *Zones) Run(args []string) (exitCode int) {
	if len(args) > 0 {
		for _, name := range args {
			if zk, present := cf.Zones[name]; present {
				t.Ui.Output(fmt.Sprintf("%8s: %s", name, zk))
			} else {
				t.Ui.Output(fmt.Sprintf("%8s: not defined", name))
			}
		}

		return
	}

	// print all
	for name, zk := range cf.Zones {
		t.Ui.Output(fmt.Sprintf("%8s: %s", name, zk))
	}

	return

}

func (t *Zones) Synopsis() string {
	return "Print available zones defined in /etc/gafka.cf"
}

func (t *Zones) Help() string {
	help := `
Usage: gafka zones [zone ...]
`
	return strings.TrimSpace(help)
}
