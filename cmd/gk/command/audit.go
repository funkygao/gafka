package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Audit struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Audit) Run(args []string) (exitCode int) {
	return
}

func (*Audit) Synopsis() string {
	return "Audit of the message streams TODO"
}

func (this *Audit) Help() string {
	help := fmt.Sprintf(`
Usage: %s audit [options]

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
