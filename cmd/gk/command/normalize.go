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
}

func (this *Normalize) Run(args []string) (exitCode int) {
	var (
		fields, mode string
	)
	cmdFlags := flag.NewFlagSet("normalize", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&fields, "f", "", "")
	cmdFlags.StringVar(&mode, "m", "url", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	switch mode {
	case "url":
		this.normalizeUrl(strings.Split(fields, ","))
	}

	return
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
	return strings.Split(url, "?")[0]
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
     Fields index start from 0

    -m <url>
     Mode

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
