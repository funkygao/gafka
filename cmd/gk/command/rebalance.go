package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Rebalance struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Rebalance) Run(args []string) (exitCode int) {
	return
}

func (*Rebalance) Synopsis() string {
	return "Rebalance the load of brokers in a kafka cluster TODO"
}

func (this *Rebalance) Help() string {
	help := fmt.Sprintf(`
Usage: %s rebalance -z zone -c cluster [options]

	Rebalance the load of brokers in a kafka cluster

`, this.Cmd)
	return strings.TrimSpace(help)
}
