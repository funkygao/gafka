package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/config"
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

		zkzone := zk.NewZkZone(zk.DefaultConfig(config.ZonePath(zone)))
		if cluster != "" {
			this.printBrokers(zkzone.GetBrokersOfCluster(cluster))

			return
		}

		this.displayZonebrokers(zone, zkzone)

		return
	}

	// print all brokers on all zones by default
	forAllZones(func(zone string, zkzone *zk.ZkZone) {
		this.displayZonebrokers(zone, zkzone)
	})

	return

}

func (this *Brokers) displayZonebrokers(zone string, zkzone *zk.ZkZone) {
	this.Ui.Output(zone)

	// sort by cluster name
	brokersOfClusters := zkzone.GetBrokers()
	sortedClusters := make([]string, 0, len(brokersOfClusters))
	for cluster, _ := range brokersOfClusters {
		sortedClusters = append(sortedClusters, cluster)
	}
	sort.Strings(sortedClusters)
	for _, cluster := range sortedClusters {
		this.Ui.Output(strings.Repeat(" ", 4) + cluster)
		this.printBrokers(brokersOfClusters[cluster])
	}
}

func (this *Brokers) printBrokers(brokers map[string]*zk.Broker) {
	if brokers == nil || len(brokers) == 0 {
		this.Ui.Output(fmt.Sprintf("\t%s", color.Red("empty")))
		return
	}

	// sort by broker id
	sortedBrokerIds := make([]string, 0, len(brokers))
	for brokerId, _ := range brokers {
		sortedBrokerIds = append(sortedBrokerIds, brokerId)
	}
	sort.Strings(sortedBrokerIds)

	for _, brokerId := range sortedBrokerIds {
		this.Ui.Output(fmt.Sprintf("\t%8s %s", brokerId, brokers[brokerId]))
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
