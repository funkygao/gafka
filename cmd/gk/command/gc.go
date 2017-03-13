package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type GC struct {
	Ui  cli.Ui
	Cmd string
}

func (this *GC) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("gc", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	return
}

func (*GC) Synopsis() string {
	return "Garbage collection for gafka"
}

func (this *GC) Help() string {
	help := fmt.Sprintf(`
Usage: %s gc

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
