package command

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/gofmt"
)

type Comma struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Comma) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("comma", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 2
	}

	if len(args) == 0 {
		this.Ui.Error("missing <integer>")
		return 2
	}

	arg := args[len(args)-1]
	for _, n := range strings.Split(arg, ",") {
		i, err := strconv.ParseInt(n, 10, 64)
		swallow(err)
		this.Ui.Outputf("%s -> %s", n, gofmt.Comma(i))
	}

	return
}

func (*Comma) Synopsis() string {
	return "Place commas after every three orders of magnitude"
}

func (this *Comma) Help() string {
	help := fmt.Sprintf(`
Usage: %s comma <integer[,integer]>

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
