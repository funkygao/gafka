package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Perf struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Perf) Run(args []string) (exitCode int) {
	var mode string
	cmdFlags := flag.NewFlagSet("perf", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&mode, "mode", "io", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	switch mode {
	case "io":
		this.Ui.Output(fmt.Sprintf("perf record -e block:block_rq_complete -a sleep 10"))
		this.Ui.Output("perf script")
	}

	return
}

func (*Perf) Synopsis() string {
	return "Probe system low level performance problems with perf"
}

func (this *Perf) Help() string {
	help := fmt.Sprintf(`
Usage: %s perf [options]

    %s

    -mode io|cpu


`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
