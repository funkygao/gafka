package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Checkup struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Checkup) Run(args []string) (exitCode int) {
	// stale brokers
	// zk down
	// underreplicated
	return
}

func (*Checkup) Synopsis() string {
	return "Checkup of brokers and zookeepers"
}

func (this *Checkup) Help() string {
	help := fmt.Sprintf(`
Usage: %s [options]

    Checkup of brokers and zookeepers

Options:

    -z zone

    -c cluster name

`, this.Cmd)
	return strings.TrimSpace(help)
}
