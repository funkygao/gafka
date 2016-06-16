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
	lastN := int64(0)

	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}

		if line[0] != ' ' {
			// time info: Thu Jun 16 22:45:01 CST 2016
		} else {
			// offset:            -CUM Messages- 255,705,684,384
			n := strings.Split(line, "-CUM Messages-")[1]
			n = strings.Replace(n, ",", "", -1)
			offset, _ := strconv.ParseInt(n, 10, 64)
			this.Ui.Output(fmt.Sprintf("%d", offset-lastN))

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
