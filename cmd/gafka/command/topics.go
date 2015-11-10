package command

import (
	"strings"

	"github.com/funkygao/gocli"
)

type Topics struct {
	Ui cli.Ui
}

func (this *Topics) Run(args []string) (exitCode int) {
	return

}

func (*Topics) Synopsis() string {
	return "Print available brokers from Zookeeper."
}

func (*Topics) Help() string {
	help := `
Usage: gafka brokers
`
	return strings.TrimSpace(help)
}
