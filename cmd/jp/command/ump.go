package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
	//"github.com/pkg/browser"
)

type Ump struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Ump) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("ump", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if len(args) == 0 {
		this.Ui.Error("missing key")
		return 2
	}

	keys := strings.Split(args[len(args)-1], ",")
	this.showReport(keys)

	return
}

func (this *Ump) showReport(keys []string) {
}

func (*Ump) Synopsis() string {
	return "Display UMP page for a specified key"
}

func (this *Ump) Help() string {
	help := fmt.Sprintf(`
Usage: %s ump key

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
