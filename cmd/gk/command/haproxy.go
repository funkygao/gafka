package command

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
	"github.com/ryanuber/columnize"
)

type Haproxy struct {
	Ui  cli.Ui
	Cmd string

	zone string
}

func (this *Haproxy) Run(args []string) (exitCode int) {
	var top bool
	cmdFlags := flag.NewFlagSet("haproxy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.DefaultZone(), "")
	cmdFlags.BoolVar(&top, "top", true, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zone := ctx.Zone(this.zone)
	if top {
		for {
			refreshScreen()
			for _, uri := range zone.HaProxyStatsUri {
				this.fetchStats(uri)
			}

			time.Sleep(time.Second * 5)
		}
	} else {
		for _, uri := range zone.HaProxyStatsUri {
			this.fetchStats(uri)
		}
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

	if resp.StatusCode != http.StatusOK {
		swallow(fmt.Errorf("fetch[%s] stats got status: %d", resp.StatusCode))
	}

	var records map[string]map[string]int64
	reader := json.NewDecoder(resp.Body)
	err = reader.Decode(&records)
	swallow(err)

	u, err := url.Parse(statsUri)
	swallow(err)
	this.Ui.Info(u.Host)

	sortedCols := make([]string, 0)
	for k, _ := range records["pub"] {
		sortedCols = append(sortedCols, k)
	}
	sort.Strings(sortedCols)

	lines := []string{strings.Join(append([]string{"svc"}, sortedCols...), "|")}
	for svc, stats := range records {
		var vals []string

		vals = append(vals, svc)
		for _, k := range sortedCols {
			v := stats[k]

			vals = append(vals, gofmt.Comma(v))
		}

		lines = append(lines, strings.Join(vals, "|"))
	}

	this.Ui.Output(columnize.SimpleFormat(lines))
}

func (this *Haproxy) Help() string {
	help := fmt.Sprintf(`
Usage: %s haproxy [options]

    %s

Options:

    -z zone

    -top
      Top mode

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
