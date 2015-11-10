package command

import (
	"strings"

	"github.com/funkygao/gocli"
)

type Topics struct {
	Ui cli.Ui
}

func (t *Topics) Run(args []string) (exitCode int) {
	return

}

func (t *Topics) Synopsis() string {
	return "Print available brokers from Zookeeper."
}

func (t *Topics) Help() string {
	help := `
Usage: gafka brokers
`
	return strings.TrimSpace(help)
}
