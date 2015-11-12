package command

import (
	"strings"

	"github.com/funkygao/gocli"
)

type Lags struct {
	Ui cli.Ui
}

func (this *Lags) Run(args []string) (exitCode int) {
	return

}

func (*Lags) Synopsis() string {
	return "Display consumer lags TODO"
}

func (*Lags) Help() string {
	help := `
Usage: gafka lags -z zone [options]
`
	return strings.TrimSpace(help)
}
