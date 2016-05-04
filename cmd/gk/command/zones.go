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
	plain      bool
}

func (this *Zones) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("zones", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&this.ipInNumber, "n", false, "")
	cmdFlags.BoolVar(&this.plain, "plain", false, "")
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

	if this.plain {
		for _, z := range zones {
			this.Ui.Output(fmt.Sprintf("%s:", z[0]))
			this.Ui.Output(fmt.Sprintf("%s\n", z[1]))
		}
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	for _, z := range zones {
		table.Append(z)
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

    -plain
      Display in non-table format.
`, this.Cmd)
	return strings.TrimSpace(help)
}
