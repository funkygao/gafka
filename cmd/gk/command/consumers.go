package command

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/funkygao/gafka/ctx"
	"github.com/funkygao/gafka/zk"
	"github.com/funkygao/gocli"
	"github.com/funkygao/golib/color"
)

type Consumers struct {
	Ui           cli.Ui
	Cmd          string
	onlineOnly   bool
	groupPattern string
	byHost       bool
}

func (this *Consumers) Run(args []string) (exitCode int) {
	var (
		cluster string
		zone    string
	)
	cmdFlags := flag.NewFlagSet("consumers", flag.ContinueOnError)
	cmdFlags.Usage = func() { this.Ui.Output(this.Help()) }
	cmdFlags.StringVar(&zone, "z", "", "")
	cmdFlags.StringVar(&cluster, "c", "", "")
	cmdFlags.StringVar(&this.groupPattern, "g", "", "")
	cmdFlags.BoolVar(&this.onlineOnly, "l", false, "")
	cmdFlags.BoolVar(&this.byHost, "byhost", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" {
		forSortedZones(func(zkzone *zk.ZkZone) {
			if this.byHost {
				this.printConsumersByHost(zkzone, cluster)
			} else {
				this.printConsumersByGroup(zkzone, cluster)
			}

		})

		return
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	if this.byHost {
		this.printConsumersByHost(zkzone, cluster)
	} else {
		this.printConsumersByGroup(zkzone, cluster)
	}

	return
}

func (this *Consumers) printConsumersByHost(zkzone *zk.ZkZone, clusterFilter string) {
	outputs := make(map[string]map[string]map[string]int) // host: {cluster: {topic: count}}

	this.Ui.Output(color.Blue(zkzone.Name()))

	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if clusterFilter != "" && clusterFilter != zkcluster.Name() {
			return
		}

		consumerGroups := zkcluster.ConsumerGroups()
		for _, group := range consumerGroups {
			for _, c := range group {
				if _, present := outputs[c.Host()]; !present {
					outputs[c.Host()] = make(map[string]map[string]int)
				}

				if _, present := outputs[c.Host()][zkcluster.Name()]; !present {
					outputs[c.Host()][zkcluster.Name()] = make(map[string]int)
				}

				for topic, count := range c.Subscription {
					outputs[c.Host()][zkcluster.Name()][topic] += count
				}
			}
		}

	})

	sortedHosts := make([]string, 0, len(outputs))
	for host, _ := range outputs {
		sortedHosts = append(sortedHosts, host)
	}
	sort.Strings(sortedHosts)
	for _, host := range sortedHosts {
		tc := outputs[host]
		this.Ui.Output(fmt.Sprintf("%s %+v", color.Green("%22s", host), tc))
	}
}

// Print all controllers of all clusters within a zone.
func (this *Consumers) printConsumersByGroup(zkzone *zk.ZkZone, clusterFilter string) {
	this.Ui.Output(color.Blue(zkzone.Name()))
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if clusterFilter != "" && clusterFilter != zkcluster.Name() {
			return
		}

		this.Ui.Output(strings.Repeat(" ", 4) + zkcluster.Name())
		consumerGroups := zkcluster.ConsumerGroups()
		sortedGroups := make([]string, 0, len(consumerGroups))
		for group, _ := range consumerGroups {
			if !patternMatched(group, this.groupPattern) {
				continue
			}

			sortedGroups = append(sortedGroups, group)
		}

		sort.Strings(sortedGroups)
		for _, group := range sortedGroups {
			consumers := consumerGroups[group]
			if len(consumers) > 0 {
				this.Ui.Output(fmt.Sprintf("\t%s %s", color.Green("☀︎"), group))

				// sort by host
				sortedIds := make([]string, 0)
				consumersMap := make(map[string]*zk.ConsumerZnode)
				for _, c := range consumers {
					sortedIds = append(sortedIds, c.Id)
					consumersMap[c.Id] = c
				}
				sort.Strings(sortedIds)
				for _, id := range sortedIds {
					this.Ui.Output(fmt.Sprintf("\t\t%s", consumersMap[id]))
				}

			} else if !this.onlineOnly {
				this.Ui.Output(fmt.Sprintf("\t%s %s", color.Yellow("☔︎"), group))
			}
		}
	})

}

func (*Consumers) Synopsis() string {
	return "Print consumer groups from Zookeeper"
}

func (this *Consumers) Help() string {
	help := fmt.Sprintf(`
Usage: %s consumers [options]

    Print consumer groups from Zookeeper

Options:

    -z zone
      Only print kafka controllers within this zone.

    -c cluster

    -g group name pattern

    -l 
      Only show online consumer groups.

    -byhost
      Display consumer groups by consumer hosts.

`, this.Cmd)
	return strings.TrimSpace(help)
}
