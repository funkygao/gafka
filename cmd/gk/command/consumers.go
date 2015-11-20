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
	if err := cmdFlags.Parse(args); err != nil {
		return 1
	}

	if zone == "" {
		forAllZones(func(zkzone *zk.ZkZone) {
			this.printConsumers(zkzone, cluster)
		})

		return
	}

	zkzone := zk.NewZkZone(zk.DefaultConfig(zone, ctx.ZoneZkAddrs(zone)))
	this.printConsumers(zkzone, cluster)

	return
}

// Print all controllers of all clusters within a zone.
func (this *Consumers) printConsumers(zkzone *zk.ZkZone, clusterFilter string) {
	this.Ui.Output(zkzone.Name())
	zkzone.WithinClusters(func(name, path string) {
		if clusterFilter != "" && clusterFilter != name {
			return
		}

		zkcluster := zkzone.NewCluster(name)
		this.Ui.Output(strings.Repeat(" ", 4) + name)
		consumerGroups := zkcluster.ConsumerGroups()
		sortedGroups := make([]string, 0, len(consumerGroups))
		for group, _ := range consumerGroups {
			if this.groupPattern != "" && !strings.Contains(group, this.groupPattern) {
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

`, this.Cmd)
	return strings.TrimSpace(help)
}
