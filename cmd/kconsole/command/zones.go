package command

import (
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/ryanuber/columnize"
)

type Zones struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Zones) Run(args []string) (exitCode int) {
	lines := make([]string, 0)
	header := "Zone|Zookeeper"
	lines = append(lines, header)
	for _, zone := range ctx.SortedZones() {
		lines = append(lines, fmt.Sprintf("%s|%s", zone, ctx.NamedZoneZkAddrs(zone)))
	}

	this.Ui.Output(columnize.SimpleFormat(lines))

	return
}

func (*Zones) Synopsis() string {
	return "Print all zones"
}

func (this *Zones) Help() string {
	help := fmt.Sprintf(`
Usage: %s zones [zone ...]

    Print all zones
`, this.Cmd)
	return strings.TrimSpace(help)
}
