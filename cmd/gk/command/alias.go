package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Alias struct {
	Ui  cli.Ui
	Cmd string

	zone      string
	cluster   string
	recursive bool
}

func (this *Alias) Run(args []string) (exitCode int) {
	for _, cmd := range ctx.Aliases() {
		alias, _ := ctx.Alias(cmd)
		this.Ui.Output(fmt.Sprintf("%10s = %s", cmd, alias))
	}

	return
}

func (*Alias) Synopsis() string {
	return "List all active aliasess"
}

func (this *Alias) Help() string {
	help := fmt.Sprintf(`
Usage: %s alias

    List all active aliasess

`, this.Cmd)
	return strings.TrimSpace(help)
}
