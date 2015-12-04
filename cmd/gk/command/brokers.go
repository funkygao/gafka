package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Brokers struct {
	Ui        cli.Ui
	Cmd       string
	staleOnly bool
}

func (this *Brokers) Run(args []string) (exitCode int) {
	var (
		zone    string
		cluster string
	)
	cmdFlags := flag.NewFlagSet("brokers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.BoolVar(&this.staleOnly, "stale", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone != "" {
		ensureZoneValid(zone)

		zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
		if cluster != "" {
			zkcluster := zkzone.NewCluster(cluster)
			lines, _ := this.clusterBrokers(cluster, zkcluster.Brokers())
			for _, l := range lines {
				this.Ui.Output(l)
			}

			return
		}

		this.displayZoneBrokers(zkzone)

		return
	}

	// print all brokers on all zones by default
	forAllZones(func(zkzone *zk.ZkZone) {
		this.displayZoneBrokers(zkzone)
	})

	return
}

func (this *Brokers) displayZoneBrokers(zkzone *zk.ZkZone) {
	lines := make([]string, 0)
	n := 0
	zkzone.ForSortedBrokers(func(cluster string, brokers map[string]*zk.BrokerZnode) {
		outputs, count := this.clusterBrokers(cluster, brokers)
		lines = append(lines, outputs...)
		n += count
	})
	this.Ui.Output(fmt.Sprintf("%s: %d", zkzone.Name(), n))
	for _, l := range lines {
		this.Ui.Output(l)
	}
}

func (this *Brokers) clusterBrokers(cluster string, brokers map[string]*zk.BrokerZnode) ([]string, int) {
	if brokers == nil || len(brokers) == 0 {
		return []string{fmt.Sprintf("%s%s %s", strings.Repeat(" ", 4),
			cluster, color.Red("empty brokers"))}, 0
	}

	lines := make([]string, 0, len(brokers))
	lines = append(lines, strings.Repeat(" ", 4)+cluster)
	if this.staleOnly {
		// try each broker's aliveness
		n := 0
		for brokerId, broker := range brokers {
			kfk, err := sarama.NewClient([]string{broker.Addr()}, sarama.NewConfig())
			if err != nil {
				n++
				lines = append(lines, color.Yellow("%8s %s %s",
					brokerId, broker, err.Error()))
			} else {
				kfk.Close()
			}
		}

		return lines, n
	}

	// sort by broker id
	sortedBrokerIds := make([]string, 0, len(brokers))
	for brokerId, _ := range brokers {
		sortedBrokerIds = append(sortedBrokerIds, brokerId)
	}
	sort.Strings(sortedBrokerIds)

	for _, brokerId := range sortedBrokerIds {
		lines = append(lines, fmt.Sprintf("\t%8s %s", brokerId, brokers[brokerId]))
	}
	return lines, len(brokers)

}

func (*Brokers) Synopsis() string {
	return "Print online brokers from Zookeeper"
}

func (this *Brokers) Help() string {
	help := fmt.Sprintf(`
Usage: %s brokers [options]

    Print online brokers from Zookeeper.

Options:

    -z zone
      Only print brokers within a zone.

    -c cluster name
      Only print brokers of this cluster.

    -stale
      Only print stale brokers: found in zk but not connectable

`, this.Cmd)
	return strings.TrimSpace(help)
}
