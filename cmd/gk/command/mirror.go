package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/cmd/gk/command/mirror"
	"github.com/funkygao/gocli"
)

type Mirror struct {
	Ui  cli.Ui
	Cmd string

	zone1, zone2       string
	cluster1, cluster2 string
	excludes           string
	debug              bool
	compress           string
	autoCommit         bool
	bandwidthLimit     int64
	progressStep       int64
}

func (this *Mirror) Run(args []string) (exitCode int) {
	cmdFlags := flag.NewFlagSet("mirror", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&this.zone1, "z1", "", "")
	cmdFlags.StringVar(&this.zone2, "z2", "", "")
	cmdFlags.StringVar(&this.cluster1, "c1", "", "")
	cmdFlags.StringVar(&this.cluster2, "c2", "", "")
	cmdFlags.StringVar(&this.excludes, "excluded", "", "")
	cmdFlags.BoolVar(&this.debug, "debug", false, "")
	cmdFlags.StringVar(&this.compress, "compress", "", "")
	cmdFlags.Int64Var(&this.bandwidthLimit, "net", 100, "")
	cmdFlags.BoolVar(&this.autoCommit, "commit", true, "")
	cmdFlags.Int64Var(&this.progressStep, "step", 10000, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		require("-z1", "-z2", "-c1", "-c2").
		invalid(args) {
		return 2
	}

	topicsExcluded := make(map[string]struct{})
	for _, e := range strings.Split(this.excludes, ",") {
		topicsExcluded[e] = struct{}{}
	}

	cf := mirror.DefaultConfig()
	cf.ExcludedTopics = topicsExcluded
	cf.Z1 = this.zone1
	cf.Z2 = this.zone2
	cf.C1 = this.cluster1
	cf.C2 = this.cluster2
	cf.Debug = this.debug
	cf.Compress = this.compress
	cf.BandwidthLimit = this.bandwidthLimit
	cf.AutoCommit = this.autoCommit
	cf.ProgressStep = this.progressStep

	setupLogging("mirror.log", "trace", "panic")

	m := mirror.New(cf)
	return m.Main()
}

func (*Mirror) Synopsis() string {
	return "Continuously copy data between two remote Kafka clusters"
}

func (this *Mirror) Help() string {
	help := fmt.Sprintf(`
Usage: %s mirror [options]

    %s

    e,g.
    gk mirror -z1 prod -c1 logstash -z2 mirror -c2 aggregator -net 100 -step 2000

Options:

    -z1 from zone

    -z2 to zone

    -c1 from cluster

    -c2 to cluster

    -exclude comma seperated topic names

    -net bandwidth limit in Mbps
      Defaults 100Mbps.
      0 means unlimited.

    -step n
      Defaults 5000.

    -debug

    -compress <gzip|snappy>
      Defaults none.

    -commit
      Auto commit the checkpoint offset.
      Defaults true.

`, this.Cmd, this.Synopsis())
	return strings.TrimSpace(help)
}
