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
		zone    string
		cluster string
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone != "" {
		ensureZoneValid(zone)

		zkutil := zk.NewZkUtil(zk.DefaultConfig(cf.Zones[zone]))
		if cluster != "" {
			for brokerId, broker := range zkutil.GetBrokersOfCluster(cluster) {
				this.Ui.Output(fmt.Sprintf("\t%8s %s", brokerId, broker))
			}

			return
		}

		// cluster provided
		for cluster, brokers := range zkutil.GetBrokers() {
			this.Ui.Output(cluster)
			for brokerId, broker := range brokers {
				this.Ui.Output(fmt.Sprintf("\t%8s %s", brokerId, broker))
			}
		}

		return
	}

	// print all brokers on all zones by default
	forAllZones(func(zone string, zkAddrs string, zkutil *zk.ZkUtil) {
		this.Ui.Output(zone)
		for cluster, brokers := range zkutil.GetBrokers() {
			this.Ui.Output(strings.Repeat(" ", 4) + cluster)
			for brokerId, broker := range brokers {
				this.Ui.Output(fmt.Sprintf("\t%8s %s", brokerId, broker))
			}
		}
	})

	return

}

func (*Brokers) Synopsis() string {
	return "Print available brokers from Zookeeper"
}

func (*Brokers) Help() string {
	help := `
Usage: gafka brokers [options]

	Print available brokers from Zookeeper.

Options:

  -z zone
  	Only print brokers within a zone.

  -c cluster name
  	Only print brokers of this cluster.

`
	return strings.TrimSpace(help)
}
