package command

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
)

type Histogram struct {
	Ui  cli.Ui
	Cmd string

	offsetFile string
}

func (this *Histogram) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("histogram", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.offsetFile, "f", "/var/wd/topics_offsets/offsets", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	f, err := os.OpenFile(this.offsetFile, os.O_RDONLY, 0660)
	swallow(err)
	defer f.Close()

	r := bufio.NewReader(f)
	var (
		lastN = int64(0)
		tm    string
	)

	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)

		if !strings.Contains(line, "CUM Messages") {
			// time info: Thu Jun 16 22:45:01 CST 2016
			tm = line
		} else {
			// offset:            -CUM Messages- 255,705,684,384
			n := strings.Split(line, "-CUM Messages-")[1]
			n = strings.Replace(n, ",", "", -1)
			n = strings.TrimSpace(n)
			offset, err := strconv.ParseInt(n, 10, 64)
			swallow(err)
			if lastN > 0 {
				this.Ui.Output(fmt.Sprintf("%55s %15s", tm, gofmt.Comma(offset-lastN)))
			}

			lastN = offset
		}

	}

	return
}

func (*Histogram) Synopsis() string {
	return "Histogram of kafka produced messages every hour"
}

func (this *Histogram) Help() string {
	help := fmt.Sprintf(`
	Usage: %s histogram [options]

	    Histogram of kafka produced messages every hour

	Options:

	    -f offset file

	`, this.Cmd)
	return strings.TrimSpace(help)
}
