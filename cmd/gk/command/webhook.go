package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Webhook struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Webhook) Run(args []string) (exitCode int) {
	return
}

func (*Webhook) Synopsis() string {
	return "Display kateway webhooks"
}

func (this *Webhook) Help() string {
	help := fmt.Sprintf(`
Usage: %s webhook [options]

    Display kateway webhooks

`, this.Cmd)
	return strings.TrimSpace(help)
}
