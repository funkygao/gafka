package command

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/funkygao/gocli"
	gio "github.com/funkygao/golib/io"
)

type Normalize struct {
	Ui  cli.Ui
	Cmd string

	echoLine  bool
	threshold float64
}

func (this *Normalize) Run(args []string) (exitCode int) {
	var (
		fields, mode string
	)
	cmdFlags := flag.NewFlagSet("normalize", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&fields, "f", "", "")
	cmdFlags.StringVar(&mode, "m", "url", "")
	cmdFlags.Float64Var(&this.threshold, "t", 1., "")
	cmdFlags.BoolVar(&this.echoLine, "l", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	switch mode {
	case "url":
		this.normalizeUrl(strings.Split(fields, ","))
	case "ng":
		this.normalizeNginxLatency(strings.Split(fields, ","))

	}

	return
}

// cat xx | awk '{print $11,$13,$14,$15}' | gk normalize -m ng
func (this *Normalize) normalizeNginxLatency(fields []string) {
	reader := bufio.NewReader(os.Stdin)
	n, slowN := 0, 0
	parts := make([]string, 100)
	for {
		l, e := gio.ReadLine(reader)
		if e != nil {
			break
		}

		n++

		line := string(l)
		tuples := strings.Fields(line)
		if len(tuples) < 1 {
			// empty line, ignored
			continue
		}

		// strip '"'
		for i, f := range tuples {
			tuples[i] = strings.Replace(f, `"`, "", -1)
			tuples[i] = strings.Replace(f, ",", "", -1)
		}

		if len(fields) > 0 {
			// print specified fields
			parts = parts[0:0]
			for _, f := range fields {
				if len(f) == 0 {
					continue
				}
				i, err := strconv.Atoi(f)
				swallow(err)
				if i < 0 {
					parts = append(parts, tuples[len(tuples)+i])
				} else {
					parts = append(parts, tuples[i])
				}
			}
			if len(parts) > 0 {
				if this.echoLine {
					this.Ui.Outputf("%d %s", n, strings.Join(parts, " "))
				} else {
					this.Ui.Output(strings.Join(parts, " "))
				}
			}

			continue
		}

		// parse nginx request_time and upstream_response_time
		// 200 214 0.002 "0.001"
		if len(tuples) != 4 {
			this.Ui.Warn(line)
			continue
		}

		request_time, _ := strconv.ParseFloat(tuples[2], 64)
		upstream_response_time, _ := strconv.ParseFloat(tuples[3], 64)
		if request_time-upstream_response_time > 0.05 {
			if this.echoLine {
				this.Ui.Warnf("%d %s", n, line)
			} else {
				this.Ui.Warn(line)
			}
		} else if request_time > this.threshold {
			slowN++
			if this.echoLine {
				this.Ui.Outputf("%d %s", n, line)
			} else {
				this.Ui.Output(line)
			}
		}
	}

	if len(fields) == 0 {
		this.Ui.Outputf("%d/%d=%f%%", slowN, n, 100.*float64(slowN)/float64(n))
	}
}

func (this *Normalize) normalizeUrl(fields []string) {
	reader := bufio.NewReader(os.Stdin)
	for {
		l, e := gio.ReadLine(reader)
		if e != nil {
			break
		}

		tuples := strings.Fields(string(l))
		for _, f := range fields {
			i, _ := strconv.Atoi(f)
			tuples[i] = this.doNormalizeUrl(tuples[i])
		}
		this.Ui.Output(strings.Join(tuples, " "))
	}
}

func (*Normalize) doNormalizeUrl(url string) string {
	r := strings.Split(url, "?")[0]
	return strings.Replace(r, "//", "/", -1)
}

func (*Normalize) Synopsis() string {
	return "Normalize the input line"
}

func (this *Normalize) Help() string {
	help := fmt.Sprintf(`
Usage: %s normalize [options]

    %s

Options:

    -f comma seperated fields
     e,g. -f 1,2
     Fields index start from 0, -1 means last field

    -m <url|ng>
     Mode 

    -t nginx upstream reponse slow threshold
     Default 1.0

    -l
     Echo line number

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
