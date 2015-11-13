package command

import (
	"strings"

	"github.com/funkygao/gocli"
)

type Rebalance struct {
	Ui cli.Ui
}

func (this *Rebalance) Run(args []string) (exitCode int) {
	return
}

func (*Rebalance) Synopsis() string {
	return "Rebalance the load of brokers in a kafka cluster TODO"
}

func (*Rebalance) Help() string {
	help := `
Usage: gafka rebalance -z zone -c cluster [options]

	Rebalance the load of brokers in a kafka cluster

`
	return strings.TrimSpace(help)
}
