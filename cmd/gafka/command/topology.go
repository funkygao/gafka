package command

import (
	"strings"

	"github.com/funkygao/gocli"
)

type Topology struct {
	Ui cli.Ui
}

func (this *Topology) Run(args []string) (exitCode int) {
	return

}

func (*Topology) Synopsis() string {
	return "Print topology of kafka clusters"
}

func (*Topology) Help() string {
	help := `
Usage: gafka topology [zone ...]
`
	return strings.TrimSpace(help)
}
