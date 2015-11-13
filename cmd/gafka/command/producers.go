package command

import (
	"strings"

	"github.com/funkygao/gocli"
)

type Producers struct {
	Ui cli.Ui
}

func (this *Producers) Run(args []string) (exitCode int) {
	return
}

func (*Producers) Synopsis() string {
	return "Display online producers TODO"
}

func (*Producers) Help() string {
	help := `
Usage: gafka producers [options]

	Display online producers

`
	return strings.TrimSpace(help)
}
