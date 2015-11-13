package command

import (
	"strings"

	"github.com/funkygao/gocli"
)

type Audit struct {
	Ui cli.Ui
}

func (this *Audit) Run(args []string) (exitCode int) {
	return
}

func (*Audit) Synopsis() string {
	return "Audit of the message streams TODO"
}

func (*Audit) Help() string {
	help := `
Usage: gafka audit [options]

	Audit of the message streams

`
	return strings.TrimSpace(help)
}
