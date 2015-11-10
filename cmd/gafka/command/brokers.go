package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
)

type Brokers struct {
	Ui cli.Ui
}

// TODO dedupe
func (this *Brokers) Run(args []string) (exitCode int) {
	var (
		zone string
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone != "" {
		zkutil := zk.NewZkUtil(zk.DefaultConfig(cf.Zones[zone]))
		for cluster, brokers := range zkutil.GetBrokers() {
			this.Ui.Output(cluster)
			for brokerId, broker := range brokers {
				this.Ui.Output(fmt.Sprintf("\t%8s %s", brokerId, broker))
			}
		}

		return
	}

	// print all brokers on all zones by default
	for name, zkAddrs := range cf.Zones {
		this.Ui.Output(name)
		zkutil := zk.NewZkUtil(zk.DefaultConfig(zkAddrs))
		for cluster, brokers := range zkutil.GetBrokers() {
			this.Ui.Output(strings.Repeat(" ", 4) + cluster)
			for brokerId, broker := range brokers {
				this.Ui.Output(fmt.Sprintf("\t%8s %s", brokerId, broker))
			}
		}
	}

	return

}

func (*Brokers) Synopsis() string {
	return "Print available brokers from Zookeeper."
}

func (*Brokers) Help() string {
	help := `
Usage: gafka brokers [options]

	Print available brokers from Zookeeper.

Options:

  -z
  	Only print brokers within a zone.
`
	return strings.TrimSpace(help)
}
