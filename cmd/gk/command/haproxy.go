package command

import (
	"encoding/csv"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
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

	cols    []string
	colsMap map[string]struct{}
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

	// initialize the output columns, the order MUST match that of haproxy output
	this.cols = []string{
		"# pxname", // proxy name
		"svname",   // service name
		//"smax",     // max sessions
		"stot", // total sessions
		"bin",  // bytes in
		"bout", // bytes out
		//"dreq",  // denied requests
		//"dresp", // denied response
		//"ereq",     // request errors
		//"econ",     // connection errors
		"wredis", // redispatches (warning)
		//"rate",     // number of sessions per second over last elapsed second
		"rate_max", // max number of new sessions per second
		"hrsp_1xx", // http responses with 1xx code
		//"hrsp_2xx",
		//"hrsp_3xx",
		"hrsp_4xx",
		"hrsp_5xx",
		"cli_abrt", // number of data transfers aborted by the client
		"srv_abrt", // number of data transfers aborted by the server (inc. in eresp)
	}
	this.colsMap = make(map[string]struct{})
	for _, c := range this.cols {
		this.colsMap[c] = struct{}{}
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

	if resp.StatusCode != 200 {
		swallow(fmt.Errorf("fetch[%s] stats got status: %d", resp.StatusCode))
	}

	reader := csv.NewReader(resp.Body)
	records, err := reader.ReadAll()
	swallow(err)

	u, err := url.Parse(statsUri)
	swallow(err)
	this.Ui.Info(u.Host)
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

			if n, err := strconv.ParseInt(col, 10, 64); err == nil {
				vals = append(vals, gofmt.Comma(n))
			} else {
				vals = append(vals, col)
			}
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
