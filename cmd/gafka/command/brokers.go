package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
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
			this.printBrokers(zkutil.GetBrokersOfCluster(cluster))

			return
		}

		// of all clusters
		for cluster, brokers := range zkutil.GetBrokers() {
			this.Ui.Output(cluster)
			this.printBrokers(brokers)
		}

		return
	}

	// print all brokers on all zones by default
	forAllZones(func(zone string, zkAddrs string, zkutil *zk.ZkUtil) {
		this.Ui.Output(zone)
		for cluster, brokers := range zkutil.GetBrokers() {
			this.Ui.Output(strings.Repeat(" ", 4) + cluster)
			this.printBrokers(brokers)
		}
	})

	return

}

func (this *Brokers) printBrokers(brokers map[string]*zk.Broker) {
	if brokers == nil {
		this.Ui.Output(fmt.Sprintf("\t%s", color.Red("empty")))
	}
	for brokerId, broker := range brokers {
		this.Ui.Output(fmt.Sprintf("\t%8s %s", brokerId, broker))
	}
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
