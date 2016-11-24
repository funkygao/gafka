package command

import (
	"encoding/csv"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gocli"
	"github.com/ryanuber/columnize"
)

type Haproxy struct {
	Ui  cli.Ui
	Cmd string

	zone string

	cols    []string
	colsMap map[string]struct{}
}

func (this *Haproxy) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("haproxy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.DefaultZone(), "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	// initialize the output columns
	this.cols = []string{
		"# pxname", "svname",
		"bin", "bout",
		"scur", "req_rate", "rate",
		"hrsp_1xx", "hrsp_2xx", "hrsp_3xx", "hrsp_4xx", "hrsp_5xx",
	}
	this.colsMap = make(map[string]struct{})
	for _, c := range this.cols {
		this.colsMap[c] = struct{}{}
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
	cols := make(map[int]string) // col:name

	lines := []string{strings.Join(this.cols, "|")}
	for i, row := range records {
		if i == 0 {
			// header
			for j, col := range row {
				cols[j] = col
			}
			continue
		}

		if (row[0] != "pub" && row[0] != "sub") || row[1] == "BACKEND" || row[1] == "FRONTEND" {
			continue
		}

		var vals []string
		for j, col := range row {
			if _, present := this.colsMap[cols[j]]; !present {
				continue
			}

			vals = append(vals, col)
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

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
