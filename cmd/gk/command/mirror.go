package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gocli"
)

type Mirror struct {
	Ui  cli.Ui
	Cmd string

	zone1, zone2       string
	cluster1, cluster2 string
	excludes           string
	topicsExcluded     map[string]struct{}
}

func (this *Mirror) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("mirror", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone1, "z1", "", "")
	cmdFlags.StringVar(&this.zone2, "z2", "", "")
	cmdFlags.StringVar(&this.cluster1, "c1", "", "")
	cmdFlags.StringVar(&this.cluster2, "c2", "", "")
	cmdFlags.StringVar(&this.excludes, "excluded", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z1", "-z2", "-c1", "-c2").
		invalid(args) {
		return 2
	}

	this.topicsExcluded = make(map[string]struct{})
	for _, e := range strings.Split(this.excludes, ",") {
		this.topicsExcluded[e] = struct{}{}
	}

	return
}

func (*Mirror) Synopsis() string {
	return "Continuously copy data between two Kafka clusters"
}

func (this *Mirror) Help() string {
	help := fmt.Sprintf(`
Usage: %s mirror [options]

    Continuously copy data between two Kafka clusters

Options:

    -z1 from zone

    -z2 to zone

    -c1 from cluster

    -c2 to cluster

    -exclude comma seperated topic names

`, this.Cmd)
	return strings.TrimSpace(help)
}
