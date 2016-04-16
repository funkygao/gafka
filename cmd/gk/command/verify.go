package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Verify struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Verify) Run(args []string) (exitCode int) {
	return
}

func (*Verify) Synopsis() string {
	return "Verify pubsub clients synced with lagacy kafka"
}

func (this *Verify) Help() string {
	help := fmt.Sprintf(`
Usage: %s verify [options]

    Verify pubsub clients synced with lagacy kafka

`, this.Cmd)
	return strings.TrimSpace(help)
}
