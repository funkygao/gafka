package command

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Influx struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Influx) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("influx", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zone := ctx.Zone(ctx.DefaultZone())
	res, err := queryInfluxDB(fmt.Sprintf("http://%s", zone.InfluxAddr), "redis",
		fmt.Sprintf(`SELECT cpu, port FROM "top" WHERE time > now() - 1m AND cpu >=%d`, 10))
	swallow(err)

	for _, row := range res {
		for _, x := range row.Series {
			fmt.Printf("cols: %+v\n", x.Columns)
			for _, val := range x.Values {
				// val[0] is time
				cpu, _ := val[1].(json.Number).Float64()
				port := val[2].(string)
				fmt.Printf("      port=%s cpu=%.2f\n", port, cpu)
			}
		}
	}

	return
}

func (*Influx) Synopsis() string {
	return "InfluxDB query debugger"
}

func (this *Influx) Help() string {
	help := fmt.Sprintf(`
Usage: %s influx

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
