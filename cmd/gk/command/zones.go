package command

import (
	"flag"
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

	ipInNumber bool
}

func (this *Zones) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("zones", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&this.ipInNumber, "n", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	// print all by default
	zones := make([][]string, 0)
	defaultZone := ctx.ZkDefaultZone()
	for _, zone := range ctx.SortedZones() {
		if defaultZone == zone {
			if this.ipInNumber {
				zones = append(zones, []string{zone + "*", ctx.ZoneZkAddrs(zone)})
			} else {
				zones = append(zones, []string{zone + "*", ctx.NamedZoneZkAddrs(zone)})
			}

			continue
		}

		if this.ipInNumber {
			zones = append(zones, []string{zone, ctx.ZoneZkAddrs(zone)})
		} else {
			zones = append(zones, []string{zone, ctx.NamedZoneZkAddrs(zone)})
		}

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
Usage: %s zones [options]

    Print zones defined in $HOME/.gafka.cf

Options:

    -n
      Show network addresses as numbers.
`, this.Cmd)
	return strings.TrimSpace(help)
}
