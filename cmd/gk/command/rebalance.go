package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Rebalance struct {
	Ui   cli.Ui
	Cmd  string
	zone string
}

func (this *Rebalance) Run(args []string) (exitCode int) {
	// bin/kafka-preferred-replica-election.sh
	// bin/kafka-reassign-partitions.sh

	if validateArgs(this, this.Ui).
		require("-z").
		requireAdminRights("-z").
		invalid(args) {
		return 2
	}

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
