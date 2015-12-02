package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Get struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Get) Run(args []string) (exitCode int) {
	return
}

func (*Get) Synopsis() string {
	return "show znode data"
}

func (this *Get) Help() string {
	help := fmt.Sprintf(`
Usage: %s get <path>

`, this.Cmd)
	return strings.TrimSpace(help)
}
