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
	gozk "github.com/samuel/go-zookeeper/zk"
)

type Consumers struct {
	Ui  cli.Ui
	Cmd string

	onlineOnly   bool
	groupPattern string
	byHost       bool
	cleanup      bool
	topicPattern string
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
	cmdFlags.StringVar(&this.topicPattern, "t", "", "")
	cmdFlags.BoolVar(&this.cleanup, "cleanup", false, "")
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if validateArgs(this, this.Ui).
		requireAdminRights("-cleanup").
		invalid(args) {
		return 2
	}

	if zone == "" {
		forSortedZones(func(zkzone *zk.ZkZone) {
			switch {
			case this.cleanup:
				this.cleanupStaleConsumerGroups(zkzone, cluster)
			case this.byHost:
				this.printConsumersByHost(zkzone, cluster)
			default:
				this.printConsumersByGroup(zkzone, cluster)
			}
		})

		return
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	switch {
	case this.cleanup:
		this.cleanupStaleConsumerGroups(zkzone, cluster)
	case this.byHost:
		this.printConsumersByHost(zkzone, cluster)
	default:
		this.printConsumersByGroup(zkzone, cluster)
	}

	return
}

func (this *Consumers) cleanupStaleConsumerGroups(zkzone *zk.ZkZone, clusterPattern string) {
	// what consumer groups are safe to delete?
	// 1. not online
	// 2. have no offsets
	this.Ui.Output(color.Blue(zkzone.Name()))

	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), clusterPattern) {
			return
		}

		this.Ui.Output(strings.Repeat(" ", 4) + zkcluster.Name())
		consumerGroups := zkcluster.ConsumerGroups()
		for group, consumers := range consumerGroups {
			if len(consumers) > 0 {
				// this consumer group is online
				continue
			}

			_, _, err := zkzone.Conn().Children(zkcluster.ConsumerGroupOffsetPath(group))
			if err == nil {
				// have offsets, unsafe to delete
				continue
			}

			if err != gozk.ErrNoNode {
				// should never happen
				swallow(err)
			}

			// have no offsets, safe to delete
			yes, err := this.Ui.Ask(fmt.Sprintf("confirm to remove consumer group: %s? [y/N]", group))
			swallow(err)
			if strings.ToLower(yes) != "y" {
				this.Ui.Info(fmt.Sprintf("%s skipped", group))
				continue
			}

			// do delete this consumer group
			zkzone.DeleteRecursive(zkcluster.ConsumerGroupRoot(group))
			this.Ui.Info(fmt.Sprintf("%s deleted", group))
		}
	})
}

func (this *Consumers) printConsumersByHost(zkzone *zk.ZkZone, clusterPattern string) {
	outputs := make(map[string]map[string]map[string]int) // host: {cluster: {topic: count}}

	this.Ui.Output(color.Blue(zkzone.Name()))

	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), clusterPattern) {
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

// Print all consumers of all clusters within a zone.
func (this *Consumers) printConsumersByGroup(zkzone *zk.ZkZone, clusterPattern string) {
	this.Ui.Output(color.Blue(zkzone.Name()))
	zkzone.ForSortedClusters(func(zkcluster *zk.ZkCluster) {
		if !patternMatched(zkcluster.Name(), clusterPattern) {
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

			this.displayGroupOffsets(zkcluster, group)
		}
	})

}

func (this *Consumers) displayGroupOffsets(zkcluster *zk.ZkCluster, group string) {
	offsetMap := zkcluster.ConsumerOffsetsOfGroup(group)
	sortedTopics := make([]string, 0, len(offsetMap))
	for topic, _ := range offsetMap {
		sortedTopics = append(sortedTopics, topic)
	}
	sort.Strings(sortedTopics)

	for _, topic := range sortedTopics {
		if !patternMatched(topic, this.topicPattern) {
			continue
		}

		sortedPartitionIds := make([]string, 0, len(offsetMap[topic]))
		for partitionId, _ := range offsetMap[topic] {
			sortedPartitionIds = append(sortedPartitionIds, partitionId)
		}
		sort.Strings(sortedPartitionIds)

		for _, partitionId := range sortedPartitionIds {
			this.Ui.Output(fmt.Sprintf("\t\t%s/%s Offset:%d",
				topic, partitionId, offsetMap[topic][partitionId]))
		}
	}

}

func (*Consumers) Synopsis() string {
	return "Print high level consumer groups from Zookeeper"
}

func (this *Consumers) Help() string {
	help := fmt.Sprintf(`
Usage: %s consumers [options]

    Print high level consumer groups from Zookeeper

Options:

    -z zone
      Only print kafka controllers within this zone.

    -c cluster

    -g group name pattern

    -t topic pattern

    -l 
      Only show online consumer groups.

    -cleanup
      Cleanup the stale consumer groups after confirmation.

    -byhost
      Display consumer groups by consumer hosts.

`, this.Cmd)
	return strings.TrimSpace(help)
}
