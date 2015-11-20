package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Producers struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Producers) Run(args []string) (exitCode int) {
	return
}

func (*Producers) Synopsis() string {
	return "Display online producers TODO"
}

func (this *Producers) Help() string {
	help := fmt.Sprintf(`
Usage: %s producers [options]

    Display online producers

`, this.Cmd)
	return strings.TrimSpace(help)
}
