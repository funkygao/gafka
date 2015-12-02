package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Stat struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Stat) Run(args []string) (exitCode int) {
	return
}

func (*Stat) Synopsis() string {
	return "display znode status"
}

func (this *Stat) Help() string {
	help := fmt.Sprintf(`
Usage: %s stat <path>


`, this.Cmd)
	return strings.TrimSpace(help)
}
