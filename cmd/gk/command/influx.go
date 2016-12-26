package command

import (
	"flag"
	"fmt"
	"strings"

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

	res, err := queryInfluxDB(this.addr, "redis",
		fmt.Sprintf(`SELECT cpu, port FROM "top" WHERE time > now() - 1m AND cpu >=%d`, 30))
	swallow(err)

	for _, row := range res {
		fmt.Printf("%+v", row)
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
