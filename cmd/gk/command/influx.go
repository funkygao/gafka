package command

import (
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
	var zone, db, sql string
	cmdFlags := flag.NewFlagSet("influx", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", ctx.DefaultZone(), "")
	cmdFlags.StringVar(&db, "db", "", "")
	cmdFlags.StringVar(&sql, "sql", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	z := ctx.Zone(zone)
	res, err := queryInfluxDB(fmt.Sprintf("http://%s", z.InfluxAddr), db, sql)
	swallow(err)

	for _, row := range res {
		for _, x := range row.Series {
			this.Ui.Outputf("name: %s", x.Name)
			this.Ui.Outputf("tags: %+v", x.Tags)
			this.Ui.Outputf("cols: %+v", x.Columns)
			this.Ui.Outputf("vals:")
			for _, val := range x.Values {
				this.Ui.Outputf("%#v", val)
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
Usage: %s influx [options]

    %s

Options:

    -z zone

    -db db

    -sql sql

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
