package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/ryanuber/columnize"
)

type Zones struct {
	Ui  cli.Ui
	Cmd string

	ipInNumber bool
	plain      bool
	longFmt    bool
	influxOnly bool
	zone       string
}

func (this *Zones) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("zones", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.BoolVar(&this.ipInNumber, "n", false, "")
	cmdFlags.BoolVar(&this.plain, "plain", false, "")
	cmdFlags.BoolVar(&this.longFmt, "l", true, "")
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.BoolVar(&this.influxOnly, "i", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	// print all by default
	lines := make([]string, 0)
	var header string
	if this.longFmt {
		header = "Zone|ZkAddr|InfluxDB"
	} else {
		header = "Zone|ZkAddr"
	}
	lines = append(lines, header)

	zones := make([][]string, 0)
	defaultZone := ctx.ZkDefaultZone()
	for _, zone := range ctx.SortedZones() {
		if this.zone != "" && this.zone != zone {
			continue
		}

		influxDbAddr := ctx.ZoneInfluxdbAddr(zone)
		if this.influxOnly && influxDbAddr == "" {
			continue
		}

		if defaultZone == zone {
			if this.ipInNumber {
				if this.longFmt {
					lines = append(lines, fmt.Sprintf("%s*|%s|%s", zone, ctx.ZoneZkAddrs(zone), influxDbAddr))
				} else {
					lines = append(lines, fmt.Sprintf("%s*|%s", zone, ctx.ZoneZkAddrs(zone)))
				}
				zones = append(zones, []string{zone + "*", ctx.ZoneZkAddrs(zone), influxDbAddr})
			} else {
				if this.longFmt {
					lines = append(lines, fmt.Sprintf("%s*|%s|%s", zone, ctx.NamedZoneZkAddrs(zone), influxDbAddr))
				} else {
					lines = append(lines, fmt.Sprintf("%s*|%s", zone, ctx.NamedZoneZkAddrs(zone)))
				}
				zones = append(zones, []string{zone + "*", ctx.NamedZoneZkAddrs(zone), influxDbAddr})
			}

			continue
		}

		if this.ipInNumber {
			if this.longFmt {
				lines = append(lines, fmt.Sprintf("%s|%s|%s", zone, ctx.ZoneZkAddrs(zone), influxDbAddr))
			} else {
				lines = append(lines, fmt.Sprintf("%s|%s", zone, ctx.ZoneZkAddrs(zone)))
			}
			zones = append(zones, []string{zone, ctx.ZoneZkAddrs(zone), influxDbAddr})
		} else {
			if this.longFmt {
				lines = append(lines, fmt.Sprintf("%s|%s|%s", zone, ctx.NamedZoneZkAddrs(zone), influxDbAddr))
			} else {
				lines = append(lines, fmt.Sprintf("%s|%s", zone, ctx.NamedZoneZkAddrs(zone)))
			}
			zones = append(zones, []string{zone, ctx.NamedZoneZkAddrs(zone), influxDbAddr})
		}

	}

	if this.plain {
		for _, z := range zones {
			this.Ui.Output(fmt.Sprintf("%s:", z[0]))
			this.Ui.Output(fmt.Sprintf("%s", z[1]))
			if len(z) > 2 && len(z[2]) > 0 {
				this.Ui.Output(fmt.Sprintf("influxdb: %s", z[2]))
			}
			this.Ui.Output("")
		}
		return
	}

	if len(lines) > 1 {
		this.Ui.Output(columnize.SimpleFormat(lines))
	}

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

    -z zone

    -n
      Show network addresses as numbers.

    -l
      Use a long listing format.

    -i
      Only display zones that has InfluxDB instances.

    -plain
      Display in non-table format.
`, this.Cmd)
	return strings.TrimSpace(help)
}
