package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Mirror struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Mirror) Run(args []string) (exitCode int) {
	return
}

func (*Mirror) Synopsis() string {
	return "Continuously copy data between two Kafka clusters"
}

func (this *Mirror) Help() string {
	help := fmt.Sprintf(`
Usage: %s mirror [options]

    Continuously copy data between two Kafka clusters

`, this.Cmd)
	return strings.TrimSpace(help)
}
