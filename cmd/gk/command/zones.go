package command

import (
	"fmt"
	"os"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/olekukonko/tablewriter"
)

type Zones struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Zones) Run(args []string) (exitCode int) {
	if len(args) > 0 {
		// header
		this.Ui.Output(fmt.Sprintf("%8s %-70s", "zone", "zookeeper ensemble"))
		this.Ui.Output(fmt.Sprintf("%s %s",
			strings.Repeat("-", 8),
			strings.Repeat("-", 70)))

		// user specified the zones to print
		for _, zone := range args {
			if zk, present := ctx.Zones()[zone]; present {
				this.Ui.Output(fmt.Sprintf("%8s %s", zone, zk))
			} else {
				this.Ui.Output(fmt.Sprintf("%8s not defined", zone))
			}
		}

		return
	}

	// print all by default
	zones := make([][]string, 0)
	defaultZone := ctx.ZkDefaultZone()
	for _, zone := range ctx.SortedZones() {
		if defaultZone == zone {
			zones = append(zones, []string{zone + "*", ctx.NamedZoneZkAddrs(zone)})
			continue
		}

		zones = append(zones, []string{zone, ctx.NamedZoneZkAddrs(zone)})
	}

	table := tablewriter.NewWriter(os.Stdout)
	for _, v := range zones {
		table.Append(v)
	}
	table.SetHeader([]string{"Zone", "ZK ensemble"})
	table.SetFooter([]string{"Total", fmt.Sprintf("%d", len(zones))})
	table.Render() // Send output

	return

}

func (*Zones) Synopsis() string {
	return "Print zones defined in $HOME/.gafka.cf"
}

func (this *Zones) Help() string {
	help := fmt.Sprintf(`
Usage: %s zones [zone ...]

    Print zones defined in $HOME/.gafka.cf
`, this.Cmd)
	return strings.TrimSpace(help)
}
