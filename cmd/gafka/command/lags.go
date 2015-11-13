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

	// /$cluster/consumers/$group/offsets/$topic/$partition
	// if > 1000, red

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
