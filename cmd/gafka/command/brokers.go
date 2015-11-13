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

		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, config.ZonePath(zone)))
		if cluster != "" {
			zkcluster := zkzone.NewCluster(cluster)
			this.printBrokers(zkcluster.Brokers())

			return
		}

		this.displayZoneBrokers(zone, zkzone)

		return
	}

	// print all brokers on all zones by default
	forAllZones(func(zone string, zkzone *zk.ZkZone) {
		this.displayZoneBrokers(zone, zkzone)
	})

	return

}

func (this *Brokers) displayZoneBrokers(zone string, zkzone *zk.ZkZone) {
	this.Ui.Output(zone)

	n := 0
	zkzone.WithinBrokers(func(cluster string, brokers map[string]*zk.Broker) {
		n += len(brokers)
		this.Ui.Output(strings.Repeat(" ", 4) + cluster)
		this.printBrokers(brokers)
	})
	this.Ui.Output(fmt.Sprintf("%80d", n))
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
	return "Print online brokers from Zookeeper"
}

func (*Brokers) Help() string {
	help := `
Usage: gafka brokers [options]

	Print online brokers from Zookeeper.

Options:

  -z zone
  	Only print brokers within a zone.

  -c cluster name
  	Only print brokers of this cluster.

`
	return strings.TrimSpace(help)
}
