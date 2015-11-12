package command

import (
	"strings"

	"github.com/funkygao/gocli"
)

type Top struct {
	Ui cli.Ui
}

func (this *Top) Run(args []string) (exitCode int) {
	return

}

func (*Top) Synopsis() string {
	return "Display top kafka cluster activities TODO"
}

func (*Top) Help() string {
	help := `
Usage: gafka top [zone ...]
`
	return strings.TrimSpace(help)
}
