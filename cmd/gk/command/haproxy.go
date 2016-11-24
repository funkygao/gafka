package command

import (
	"encoding/csv"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
)

type Haproxy struct {
	Ui  cli.Ui
	Cmd string

	zone string
}

func (this *Haproxy) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("haproxy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.DefaultZone(), "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zone := ctx.Zone(this.zone)
	for _, uri := range zone.HaProxyStatsUri {
		this.fetchStats(uri)
	}

	return
}

func (*Haproxy) Synopsis() string {
	return "Query ehaproxy cluster for load stats"
}

func (this *Haproxy) fetchStats(statsUri string) {
	client := http.Client{Timeout: time.Second * 30}
	resp, err := client.Get(statsUri)
	swallow(err)
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		swallow(fmt.Errorf("fetch[%s] stats got status: %d", resp.StatusCode))
	}

	reader := csv.NewReader(resp.Body)
	records, err := reader.ReadAll()
	swallow(err)

	this.Ui.Info(statsUri)
	for _, x := range records {
		this.Ui.Output(fmt.Sprintf("%+v", x))
	}
}

func (this *Haproxy) Help() string {
	help := fmt.Sprintf(`
Usage: %s haproxy [options]

    %s

Options:

    -z zone

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
