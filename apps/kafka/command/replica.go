package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Replica struct {
	Ui  cli.Ui
	Cmd string
}

func (this *Replica) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("replica", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	this.Ui.Output(`
HighWatermark LogEndOffset
HW is updated only by leader of a partition.
Each replica has a LEO.

Leader of a partition manages replicas state in memory, replicas themselves dont.        


		`)

	return
}

func (*Replica) Synopsis() string {
	return "Explains kafka replica mechanism"
}

func (this *Replica) Help() string {
	help := fmt.Sprintf(`
Usage: %s replica [options]

    %s

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
