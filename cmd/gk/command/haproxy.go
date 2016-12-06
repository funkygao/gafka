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
	"github.com/funkygao/golib/top"
	"github.com/ryanuber/columnize"
)

type Haproxy struct {
	Ui  cli.Ui
	Cmd string

	zone string
}

func (this *Haproxy) Run(args []string) (exitCode int) {
	var topMode bool
	cmdFlags := flag.NewFlagSet("haproxy", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", ctx.DefaultZone(), "")
	cmdFlags.BoolVar(&topMode, "top", true, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	zone := ctx.Zone(this.zone)
	if topMode {
		header, _ := this.getStats(zone.HaProxyStatsUri[0])
		t := top.New(header, "%8s %4s %15s %15s %8s %6s %8s %10s %8s %8s %5s %7s %9s %6s")
		go func() {
			for {
				rows := make([]string, 0)
				for _, uri := range zone.HaProxyStatsUri {
					_, r := this.getStats(uri)
					rows = append(rows, r...)
				}
				t.Refresh(rows)

				time.Sleep(time.Second * 5)
			}
		}()
		if err := t.Start(); err != nil {
			panic(err)
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

func (this *Haproxy) getStats(statsUri string) (header string, rows []string) {
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
	var shortHostname string
	if strings.Contains(u.Host, ":") {
		u.Host = u.Host[:strings.Index(u.Host, ":")]
	}
	tuples := strings.SplitN(u.Host, ".", 4)
	if len(tuples) < 4 {
		shortHostname = u.Host
	} else {
		shortHostname = tuples[3]
	}
	if len(shortHostname) > 8 {
		shortHostname = shortHostname[:8]
	}

	sortedSvcs := make([]string, 0)
	for svc, _ := range records {
		sortedSvcs = append(sortedSvcs, svc)
	}
	sort.Strings(sortedSvcs)

	sortedCols := make([]string, 0)
	for k, _ := range records["pub"] {
		sortedCols = append(sortedCols, k)
	}
	sort.Strings(sortedCols)

	header = strings.Join(append([]string{"host", "svc"}, sortedCols...), "|")
	for _, svc := range sortedSvcs {
		stats := records[svc]

		var vals = []string{shortHostname, svc}
		for _, k := range sortedCols {
			v := stats[k]

			vals = append(vals, gofmt.Comma(v))
		}

		rows = append(rows, strings.Join(vals, "|"))
	}

	return
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

	sortedSvcs := make([]string, 0)
	for svc, _ := range records {
		sortedSvcs = append(sortedSvcs, svc)
	}
	sort.Strings(sortedSvcs)

	sortedCols := make([]string, 0)
	for k, _ := range records["pub"] {
		sortedCols = append(sortedCols, k)
	}
	sort.Strings(sortedCols)

	lines := []string{strings.Join(append([]string{"svc"}, sortedCols...), "|")}
	for _, svc := range sortedSvcs {
		stats := records[svc]

		var vals = []string{svc}
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
