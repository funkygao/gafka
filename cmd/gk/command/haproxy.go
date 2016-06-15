package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Haproxy struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Haproxy) Run(args []string) (exitCode int) {

	return
}

func (*Haproxy) Synopsis() string {
	return "Query haproxy cluster for load stats"
}

func (this *Haproxy) Help() string {
	help := fmt.Sprintf(`
Usage: %s haproxy [options]

    Query haproxy cluster for load stats

`, this.Cmd)
	return strings.TrimSpace(help)
}
