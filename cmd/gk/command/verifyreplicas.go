package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type VerifyReplicas struct {
	Ui  cli.Ui
	Cmd string

	zone    string
	cluster string
	topic   string
}

func (this *VerifyReplicas) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("verifyreplicas", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone, "z", "", "")
	cmdFlags.StringVar(&this.cluster, "c", "", "")
	cmdFlags.StringVar(&this.topic, "t", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z", "-c").
		invalid(args) {
		return 2
	}

	return
}

func (*VerifyReplicas) Synopsis() string {
	return "Validate that all replicas for a set of topics have the same data TODO"
}

func (this *VerifyReplicas) Help() string {
	help := fmt.Sprintf(`
Usage: %s verifyreplicas [options]

    Validate that all replicas for a set of topics have the same data

Options:

    -z zone

    -c cluster

    -t topic

`, this.Cmd)
	return strings.TrimSpace(help)
}
