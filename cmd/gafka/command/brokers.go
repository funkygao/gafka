package command

import (
	"strings"

	"github.com/funkygao/gocli"
)

type Brokers struct {
	Ui cli.Ui
}

func (b *Brokers) Run(args []string) (exitCode int) {
	return

}

func (b *Brokers) Synopsis() string {
	return "Print available brokers from Zookeeper."
}

func (b *Brokers) Help() string {
	help := `
Usage: gafka brokers [options]

	Print available brokers from Zookeeper.

Options:

  -format                  If provided, output is returned in the specified
                           format. Valid formats are 'json', and 'text' (default)

  -rpc-addr=127.0.0.1:7373 RPC address of the Serf agent.

  -rpc-auth=""             RPC auth token of the Serf agent.

`
	return strings.TrimSpace(help)
}
