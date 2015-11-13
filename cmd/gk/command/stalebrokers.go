package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type StaleBrokers struct {
	Ui  cli.Ui
	Cmd string
}

func (this *StaleBrokers) Run(args []string) (exitCode int) {
	return
}

func (*StaleBrokers) Synopsis() string {
	return "Display stale brokers TODO"
}

func (this *StaleBrokers) Help() string {
	help := fmt.Sprintf(`
Usage: %s stalebrokers [options]

	Display stale brokers: found in Zookeeper but not able to connect

`, this.Cmd)
	return strings.TrimSpace(help)
}
