package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Merge struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Merge) Run(args []string) (exitCode int) {
	if len(args) == 0 {
		this.Ui.Error("missing path")
		this.Ui.Output(this.Help())
		return 2
	}

	return
}

func (*Merge) Synopsis() string {
	return "Visualize segments merge process"
}

func (this *Merge) Help() string {
	help := fmt.Sprintf(`
Usage: %s merge <path>

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
